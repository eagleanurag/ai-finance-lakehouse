from __future__ import annotations

import logging
from typing import Any

from config import settings
from src.warehouses.duckdb_store import GOLD_TABLES

log = logging.getLogger(__name__)


def sync_bigquery() -> dict[str, Any]:
    if not settings.BIGQUERY_PROJECT:
        return {"status": "skipped", "reason": "BIGQUERY_PROJECT not set"}
    try:
        from google.cloud import bigquery
    except ImportError:
        return {
            "status": "skipped",
            "reason": "google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery",
        }
    try:
        import duckdb

        client = bigquery.Client(project=settings.BIGQUERY_PROJECT)
        local = duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)
        results: dict[str, int] = {}
        dataset_id = "finance_lakehouse"
        dataset_ref = bigquery.Dataset(f"{settings.BIGQUERY_PROJECT}.{dataset_id}")
        dataset_ref.location = "US"
        try:
            client.create_dataset(dataset_ref, exists_ok=True)
        except Exception:
            pass
        for table in GOLD_TABLES:
            df = local.execute(f"SELECT * FROM gold.{table}").fetchdf()
            df.columns = [c.lower() for c in df.columns]
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            if "date" in table or "month" in table:
                for col in df.columns:
                    if str(df[col].dtype).startswith("datetime"):
                        continue
            job = client.load_table_from_dataframe(
                df, f"{dataset_id}.{table}", job_config=job_config
            )
            job.result()
            results[table] = len(df)
        local.close()
        return {"status": "ok", "tables": results}
    except Exception as exc:
        log.exception("BigQuery sync failed")
        return {"status": "error", "reason": str(exc)}


def sync_mssql() -> dict[str, Any]:
    if not settings.MSSQL_DSN:
        return {"status": "skipped", "reason": "MSSQL_DSN not set"}
    try:
        import pyodbc
    except ImportError:
        return {"status": "skipped", "reason": "pyodbc not installed. Run: pip install pyodbc"}
    try:
        import duckdb

        conn = pyodbc.connect(settings.MSSQL_DSN, timeout=30)
        cursor = conn.cursor()
        local = duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)
        results: dict[str, int] = {}
        for table in GOLD_TABLES:
            df = local.execute(f"SELECT * FROM gold.{table}").fetchdf()
            df.columns = [c.lower() for c in df.columns]
            cols_sql = ", ".join(f"[{c}] NVARCHAR(MAX)" for c in df.columns)
            cursor.execute(f"IF OBJECT_ID('{table}','U') IS NOT NULL DROP TABLE {table}")
            cursor.execute(f"CREATE TABLE {table} ({cols_sql})")
            placeholders = ", ".join("?" * len(df.columns))
            cursor.fast_executemany = True
            cursor.executemany(
                f"INSERT INTO {table} VALUES ({placeholders})",
                [tuple(None if pd_isna(v) else v for v in row) for row in df.itertuples(index=False, name=None)],
            )
            conn.commit()
            results[table] = len(df)
        local.close()
        conn.close()
        return {"status": "ok", "tables": results}
    except Exception as exc:
        log.exception("MSSQL sync failed")
        return {"status": "error", "reason": str(exc)}


def pd_isna(value) -> bool:
    try:
        import pandas as pd

        return pd.isna(value)
    except Exception:
        return value is None
