from __future__ import annotations

import logging
from typing import Any

from config import settings

log = logging.getLogger(__name__)

def gold_tables(max_rows: int | None = None) -> tuple[list[str], list[str]]:
    """Gold tables to sync, and those skipped because they exceed max_rows (free-tier OLTP targets)."""
    import duckdb

    con = duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)
    try:
        rows = con.execute(
            "SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name = 'gold' ORDER BY 1"
        ).fetchall()
    finally:
        con.close()
    keep = [t for t, n in rows if max_rows is None or n <= max_rows]
    return keep, [t for t, _ in rows if t not in keep]


def sync_motherduck() -> dict[str, Any]:
    if not settings.MOTHERDUCK_TOKEN:
        return {"status": "skipped", "reason": "MOTHERDUCK_TOKEN not set"}
    import duckdb

    tables, _ = gold_tables()
    try:
        md = duckdb.connect(f"md:?motherduck_token={settings.MOTHERDUCK_TOKEN}")
        try:
            md.execute("CREATE DATABASE IF NOT EXISTS finance_lakehouse")
            md.execute("USE finance_lakehouse")
            md.execute(f"ATTACH '{settings.DUCKDB_PATH.as_posix()}' AS local_wh (READ_ONLY)")
            results: dict[str, Any] = {}
            for table in tables:
                md.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM local_wh.gold.{table}")
                results[table] = md.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
            return {"status": "ok", "tables": results}
        finally:
            md.close()
    except Exception as exc:
        log.exception("MotherDuck sync failed")
        return {"status": "error", "reason": str(exc)}
