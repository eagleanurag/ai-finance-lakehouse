from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from config import settings
from src.pipelines.bronze import iter_bronze_files, read_bronze_file

log = logging.getLogger(__name__)

EMPTY_SCHEMAS: dict[str, dict[str, str]] = {
    "transactions": {
        "transaction_id": "VARCHAR",
        "event_ts": "VARCHAR",
        "event_date": "VARCHAR",
        "dept_code": "VARCHAR",
        "account_code": "VARCHAR",
        "vendor_name": "VARCHAR",
        "amount_usd": "DOUBLE",
        "currency": "VARCHAR",
        "txn_type": "VARCHAR",
        "source_channel": "VARCHAR",
        "load_batch": "VARCHAR",
    },
    "invoices": {
        "invoice_number": "VARCHAR",
        "invoice_date": "VARCHAR",
        "vendor_name_guess": "VARCHAR",
        "total_amount_usd": "DOUBLE",
        "tax_amount_usd": "DOUBLE",
        "ocr_engine": "VARCHAR",
        "source_channel": "VARCHAR",
        "source_file": "VARCHAR",
        "raw_text_chars": "BIGINT",
        "load_batch": "VARCHAR",
    },
    "budgets": {
        "budget_id": "VARCHAR",
        "year_month": "VARCHAR",
        "dept_code": "VARCHAR",
        "account_code": "VARCHAR",
        "budget_amount_usd": "DOUBLE",
        "source_channel": "VARCHAR",
        "load_batch": "VARCHAR",
    },
    "departments_src": {
        "dept_code": "VARCHAR",
        "dept_name": "VARCHAR",
        "cost_center": "VARCHAR",
        "headcount": "BIGINT",
        "load_batch": "VARCHAR",
    },
    "gl_accounts_src": {
        "account_code": "VARCHAR",
        "account_name": "VARCHAR",
        "category": "VARCHAR",
        "load_batch": "VARCHAR",
    },
    "vendors_src": {
        "vendor_name": "VARCHAR",
        "vendor_category": "VARCHAR",
        "country": "VARCHAR",
        "load_batch": "VARCHAR",
    },
}


def _create_empty(con: duckdb.DuckDBPyConnection, table: str) -> None:
    schema = EMPTY_SCHEMAS.get(table)
    con.execute(f"DROP TABLE IF EXISTS raw.{table}")
    if schema is None:
        con.execute(f"CREATE TABLE raw.{table} (load_batch VARCHAR)")
        return
    cols = ", ".join(f'"{col}" {typ}' for col, typ in schema.items())
    con.execute(f"CREATE TABLE raw.{table} ({cols})")


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    settings.ensure_dirs()
    return duckdb.connect(str(settings.DUCKDB_PATH), read_only=read_only)


def _load_entity(
    con: duckdb.DuckDBPyConnection,
    table: str,
    bronze_dir: Path,
    entity_filter: str | None = None,
) -> int:
    frames: list[pd.DataFrame] = []
    for path in iter_bronze_files(bronze_dir):
        if entity_filter:
            rel_parts = path.relative_to(bronze_dir).parts
            if not rel_parts or rel_parts[0] != entity_filter:
                continue
        records = read_bronze_file(path)
        if not records:
            continue
        df = pd.DataFrame(records)
        df["load_batch"] = path.stem + "|" + path.parent.name
        frames.append(df)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    if not frames:
        _create_empty(con, table)
        return 0
    combined = pd.concat(frames, ignore_index=True, sort=False)
    try:
        con.execute(f"DROP TABLE IF EXISTS raw.{table}")
        con.register("_tmp_load", combined)
        con.execute(f"CREATE TABLE raw.{table} AS SELECT * FROM _tmp_load")
    finally:
        con.unregister("_tmp_load")
    return len(combined)


def run_transforms() -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    con = get_connection()
    counts: dict[str, int] = {}
    try:
        counts["transactions"] = _load_entity(con, "transactions", settings.BRONZE_TXN_DIR)
        counts["invoices"] = _load_entity(con, "invoices", settings.BRONZE_INVOICE_DIR)
        counts["budgets"] = _load_entity(con, "budgets", settings.BRONZE_BUDGET_DIR)
        for master_dir in sorted(settings.BRONZE_MASTER_DIR.glob("*")):
            if not master_dir.is_dir():
                continue
            table = f"{master_dir.name}_src"
            counts[table] = _load_entity(con, table, settings.BRONZE_MASTER_DIR, entity_filter=master_dir.name)

        if counts.get("transactions", 0) == 0:
            raise RuntimeError(
                "No bronze transactions found. Run the ingestion pipeline first "
                "(python -m run_pipeline --full)."
            )

        executed: list[str] = []
        for sql_file in sorted(settings.SQL_MODELS_DIR.glob("*.sql")):
            con.execute(sql_file.read_text(encoding="utf-8"))
            executed.append(sql_file.name)
            log.info("Executed %s", sql_file.name)

        gold_tables = [
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold'"
            ).fetchall()
        ]
        final_counts = {
            t: con.execute(f"SELECT COUNT(*) FROM gold.{t}").fetchone()[0] for t in sorted(gold_tables)
        }
    finally:
        con.close()

    meta = {
        "last_transform_utc": started.isoformat(),
        "bronze_rows_loaded": counts,
        "models_executed": executed,
        "gold_row_counts": final_counts,
    }
    meta_path = settings.DATA_DIR / "transform_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    log.info("Transform complete: %s", json.dumps(final_counts))
    return meta


def query_gold(sql: str, limit: int = 500) -> pd.DataFrame:
    con = get_connection()
    try:
        return con.execute(sql).fetchdf().head(limit)
    finally:
        con.close()


def table_schemas() -> list[dict[str, str]]:
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema IN ('gold', 'silver')
            ORDER BY table_schema, table_name, ordinal_position
            """
        ).fetchall()
    finally:
        con.close()
    return [
        {"table": f"{r[0]}.{r[1]}", "column": r[2], "type": r[3]}
        for r in rows
    ]


def sample_rows(table: str, n: int = 3) -> list[dict[str, Any]]:
    allowed = {f"{s}.{t}" for s in ("gold", "silver") for t in (
        "silver_transactions", "silver_budgets", "silver_invoices",
        "dim_date", "dim_department", "dim_gl_account", "dim_vendor",
        "fact_transactions", "fact_budget", "kpi_monthly_actual_vs_budget", "monthly_financials",
    )}
    if table not in allowed:
        raise ValueError(f"Table {table} is not accessible")
    con = get_connection()
    try:
        df = con.execute(f"SELECT * FROM {table} LIMIT {n}").fetchdf()
    finally:
        con.close()
    return df.to_dict(orient="records")
