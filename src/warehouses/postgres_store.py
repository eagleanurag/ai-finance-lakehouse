from __future__ import annotations

import logging
from typing import Any

from config import settings
from src.warehouses.duckdb_store import GOLD_TABLES

log = logging.getLogger(__name__)


def sync_postgres() -> dict[str, Any]:
    if not settings.POSTGRES_DSN:
        return {"status": "skipped", "reason": "POSTGRES_DSN not set (Neon/Supabase)"}
    try:
        import psycopg
    except ImportError:
        return {
            "status": "skipped",
            "reason": "psycopg not installed. Run: pip install 'psycopg[binary]'",
        }
    try:
        import duckdb
        import pandas as pd

        local = duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)
        results: dict[str, int] = {}
        with psycopg.connect(settings.POSTGRES_DSN, autocommit=True) as conn:
            for table in GOLD_TABLES:
                df = local.execute(f"SELECT * FROM gold.{table}").fetchdf()
                df.columns = [c.lower() for c in df.columns]
                conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                cols_sql = ", ".join(
                    f'"{c}" {__pg_type(str(t))}' for c, t in zip(df.columns, df.dtypes)
                )
                conn.execute(f'CREATE TABLE "{table}" ({cols_sql})')
                if not df.empty:
                    with conn.cursor() as cur:
                        with cur.copy(f'COPY "{table}" FROM STDIN') as copy:
                            for record in df.itertuples(index=False, name=None):
                                copy.write_row(record)
                results[table] = len(df)
        local.close()
        return {"status": "ok", "tables": results}
    except Exception as exc:
        log.exception("Postgres sync failed")
        return {"status": "error", "reason": str(exc)}


def __pg_type(duck_type: str) -> str:
    d = duck_type.lower()
    if "int" in d:
        return "BIGINT"
    if "float" in d or "decimal" in d:
        return "DOUBLE PRECISION"
    if "date" == d:
        return "DATE"
    if "timestamp" in d or "datetime" in d:
        return "TIMESTAMPTZ"
    if "bool" in d:
        return "BOOLEAN"
    return "TEXT"
