from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import date, datetime, timezone
from typing import Any

import duckdb
import pandas as pd

from config import settings
from src.pipelines.notebook_runner import NotebookError, run_notebook

log = logging.getLogger(__name__)


def get_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Read access to the live warehouse (builds write to a staging file, never through this)."""
    settings.ensure_dirs()
    return duckdb.connect(str(settings.DUCKDB_PATH), read_only=read_only)


def _staging_path():
    return settings.DUCKDB_PATH.with_name(settings.DUCKDB_PATH.stem + ".staging.duckdb")


def run_transforms(save_outputs: bool = False) -> dict[str, Any]:
    """Run bronze->silver->gold notebooks into a staging warehouse; publish it only if gold tests pass."""
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    staging = _staging_path()
    for leftover in (staging, staging.with_name(staging.name + ".wal")):
        leftover.unlink(missing_ok=True)

    params = {
        "LAKE_DIR": str(settings.LAKE_DIR),
        "DB_PATH": str(staging),
        "PREVIOUS_DB_PATH": str(settings.DUCKDB_PATH),
        "RUN_ID": run_id,
        "AS_OF_DATE": date.today().isoformat(),
        "MEMORY_LIMIT": settings.DUCKDB_MEMORY_LIMIT,
    }
    executed: list[dict[str, Any]] = []
    status, detail = "success", ""
    try:
        for name in settings.TRANSFORM_NOTEBOOKS:
            nb_params = params if name.startswith("01") else {k: params[k] for k in ("DB_PATH", "RUN_ID", "AS_OF_DATE", "MEMORY_LIMIT")}
            result = run_notebook(settings.NOTEBOOKS_DIR / name, nb_params, save_outputs=save_outputs)
            executed.append(result)
            log.info("Executed %s in %ss", name, result["seconds"])
    except NotebookError as exc:
        status, detail = "failed", str(exc)
        log.error("Transform failed - live warehouse left untouched: %s", exc)

    con = duckdb.connect(str(staging))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS audit")
        con.execute("""CREATE TABLE IF NOT EXISTS audit.pipeline_runs (run_id VARCHAR, notebook VARCHAR,
            started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, seconds DOUBLE, status VARCHAR, detail VARCHAR)""")
        con.execute("""CREATE TABLE IF NOT EXISTS audit.dq_results (run_id VARCHAR, layer VARCHAR, entity VARCHAR,
            check_name VARCHAR, severity VARCHAR, failed_rows BIGINT, total_rows BIGINT, pass_pct DOUBLE,
            action VARCHAR, checked_at TIMESTAMPTZ)""")
        for res in executed:
            con.execute("INSERT INTO audit.pipeline_runs VALUES (?, ?, ?, now(), ?, 'success', '')",
                        [run_id, res["notebook"], started, res["seconds"]])
        if status != "success":
            con.execute("INSERT INTO audit.pipeline_runs VALUES (?, 'pipeline', ?, now(), NULL, 'failed', ?)",
                        [run_id, started, detail])
        gold_counts = dict(con.execute(
            "SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name = 'gold' ORDER BY 1").fetchall())
        silver_counts = dict(con.execute(
            "SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name = 'silver' ORDER BY 1").fetchall())
        dq = con.execute("""
            SELECT layer, count(*) FILTER (WHERE failed_rows > 0) AS checks_with_findings,
                   sum(failed_rows) FILTER (WHERE action = 'quarantined') AS quarantined_rows,
                   count(*) FILTER (WHERE severity = 'error' AND failed_rows > 0 AND layer = 'gold') AS failed_tests
            FROM audit.dq_results WHERE run_id = ? GROUP BY layer""", [run_id]).fetchall()
        con.execute("CHECKPOINT")
    finally:
        con.close()

    if status != "success":
        raise RuntimeError(detail)

    os.replace(staging, settings.DUCKDB_PATH)
    settings.DUCKDB_PATH.with_name(settings.DUCKDB_PATH.name + ".wal").unlink(missing_ok=True)

    meta = {
        "last_transform_utc": started.isoformat(),
        "run_id": run_id,
        "notebooks_executed": executed,
        "silver_row_counts": silver_counts,
        "gold_row_counts": gold_counts,
        "data_quality": {layer: {"checks_with_findings": f, "quarantined_rows": q or 0, "failed_tests": t}
                         for layer, f, q, t in dq},
    }
    (settings.DATA_DIR / "transform_meta.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    log.info("Transform complete: %s", json.dumps(gold_counts))
    return meta


def query_gold(sql: str, limit: int = 500) -> pd.DataFrame:
    con = get_connection()
    try:
        return con.execute(sql).fetchdf().head(limit)
    finally:
        con.close()


def table_schemas(schemas: tuple[str, ...] = ("gold",)) -> list[dict[str, str]]:
    """Columns the AI analyst may query: the reporting (gold) layer by default."""
    con = get_connection()
    try:
        rows = con.execute(
            f"""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema IN ({", ".join("?" for _ in schemas)})
            ORDER BY table_schema, table_name, ordinal_position
            """,
            list(schemas),
        ).fetchall()
    finally:
        con.close()
    return [
        {"table": f"{r[0]}.{r[1]}", "column": r[2], "type": r[3]}
        for r in rows
    ]


def sample_rows(table: str, n: int = 3) -> list[dict[str, Any]]:
    con = get_connection()
    try:
        allowed = {f"{s}.{t}" for s, t in con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() WHERE schema_name IN ('gold', 'silver')").fetchall()}
        if table not in allowed:
            raise ValueError(f"Table {table} is not accessible")
        df = con.execute(f"SELECT * FROM {table} LIMIT {n}").fetchdf()
    finally:
        con.close()
    return df.to_dict(orient="records")
