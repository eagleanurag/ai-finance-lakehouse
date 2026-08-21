from __future__ import annotations

import logging
from typing import Any

from config import settings

log = logging.getLogger(__name__)

GOLD_TABLES = [
    "dim_date",
    "dim_department",
    "dim_gl_account",
    "dim_vendor",
    "fact_transactions",
    "fact_budget",
    "kpi_monthly_actual_vs_budget",
    "monthly_financials",
]


def sync_motherduck() -> dict[str, Any]:
    if not settings.MOTHERDUCK_TOKEN:
        return {"status": "skipped", "reason": "MOTHERDUCK_TOKEN not set"}
    import duckdb

    local_con = duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)
    try:
        md = duckdb.connect(f"md:?motherduck_token={settings.MOTHERDUCK_TOKEN}")
        try:
            md.execute("CREATE DATABASE IF NOT EXISTS finance_lakehouse")
            md.execute("USE finance_lakehouse")
            results: dict[str, Any] = {}
            for table in GOLD_TABLES:
                rows = local_con.execute(f"SELECT * FROM gold.{table}").fetchall()
                cols = [d[0] for d in local_con.execute(f"SELECT * FROM gold.{table} LIMIT 0").description]
                df = pd_from_rows(rows, cols)
                md.register("_sync_df", df)
                md.execute("CREATE OR REPLACE TABLE _incoming AS SELECT * FROM _sync_df")
                md.execute(f"DROP TABLE IF EXISTS {table}")
                md.execute(f"CREATE TABLE {table} AS SELECT * FROM _incoming")
                md.execute("DROP TABLE _incoming")
                md.unregister("_sync_df")
                results[table] = len(df)
            return {"status": "ok", "tables": results}
        finally:
            md.close()
    except Exception as exc:
        log.exception("MotherDuck sync failed")
        return {"status": "error", "reason": str(exc)}
    finally:
        local_con.close()


def pd_from_rows(rows: list[tuple], columns: list[str]):
    import pandas as pd

    return pd.DataFrame(rows, columns=columns)
