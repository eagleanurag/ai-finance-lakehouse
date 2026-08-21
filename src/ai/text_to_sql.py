from __future__ import annotations

import re
from typing import Any

import pandas as pd

from src.pipelines.transform import get_connection, table_schemas

FORBIDDEN_KEYWORDS = [
    "insert", "update", "delete", "drop", "alter", "create", "truncate",
    "grant", "revoke", "copy", "export", "attach", "detach", "install", "load",
]

SYSTEM_PROMPT = """You are a senior financial data analyst embedded in a lakehouse platform.
You translate natural-language questions into a single safe, read-only DuckDB SQL query.

STRICT RULES:
1. Output ONLY the SQL query. No explanations, no markdown fences.
2. Only SELECT statements. Never use INSERT/UPDATE/DELETE/DDL.
3. Use only tables and columns listed in the schema below.
4. Always include an ORDER BY when returning time series or rankings.
5. Limit results to at most 200 rows unless aggregation makes that impossible.
6. DuckDB dialect: use date_trunc, strftime, SUM CASE WHEN, QUALIFY as needed.
7. Amount columns are in USD. month_start is DATE; year_month strings look like '2026-01'.

SCHEMA:
"""


def build_schema_context() -> str:
    rows = table_schemas()
    grouped: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row["table"], []).append((row["column"], row["type"]))
    lines = []
    for table, cols in sorted(grouped.items()):
        col_lines = ",\n  ".join(f"{c} {t}" for c, t in cols)
        lines.append(f"{table}(\n  {col_lines}\n)")
    return "\n\n".join(lines)


def sanitize_sql(raw_sql: str) -> str:
    sql = raw_sql.strip()
    sql = re.sub(r"^```(?:sql)?|```$", "", sql.strip(), flags=re.IGNORECASE | re.MULTILINE).strip()
    if ";" in sql.rstrip(";"):
        raise ValueError("Multiple SQL statements are not allowed")
    sql = sql.rstrip(";").strip()
    lowered = sql.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Only SELECT / WITH queries are allowed")
    for kw in FORBIDDEN_KEYWORDS:
        pattern = rf"\b{kw}\b"
        if re.search(pattern, lowered):
            raise ValueError(f"Forbidden keyword detected: {kw}")
    return sql


def enforce_limit(sql: str, max_rows: int = 200) -> str:
    if re.search(r"\blimit\s+\d+\b", sql.lower()):
        return sql
    return f"SELECT * FROM (\n{sql}\n) _sub LIMIT {max_rows}"


def execute_text_to_sql(question: str, provider_key: str = "auto") -> dict[str, Any]:
    schema_ctx = build_schema_context()
    system_prompt = SYSTEM_PROMPT + schema_ctx
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": question},
    ]

    from src.ai.router import chat

    llm_result = chat(messages, provider_key=provider_key)
    attempts_log = {"llm_attempts": llm_result.get("attempts", [])}
    if not llm_result.get("content"):
        return {"error": llm_result.get("error", "LLM call failed"), **attempts_log}

    try:
        sql = enforce_limit(sanitize_sql(llm_result["content"]))
    except ValueError as exc:
        retry_messages = messages + [
            {"role": "assistant", "content": llm_result["content"]},
            {
                "role": "user",
                "content": f"Your SQL was rejected: {exc}. Rewrite it following ALL rules. Output ONLY corrected SQL.",
            },
        ]
        retry = chat(retry_messages, provider_key=provider_key)
        if not retry.get("content"):
            return {"error": f"SQL rejected and self-correction failed: {exc}", **attempts_log}
        try:
            sql = enforce_limit(sanitize_sql(retry["content"]))
        except ValueError as exc2:
            return {"error": f"Generated SQL invalid after retry: {exc2}", "sql_raw": retry["content"], **attempts_log}

    con = get_connection()
    try:
        df: pd.DataFrame = con.execute(sql).fetchdf()
    except Exception as exc:
        repair_messages = messages + [
            {"role": "assistant", "content": sql},
            {
                "role": "user",
                "content": f"That SQL failed with error: {exc}. Fix it. Output ONLY corrected SQL.",
            },
        ]
        repaired = chat(repair_messages, provider_key=provider_key)
        if not repaired.get("content"):
            return {"error": f"SQL execution failed: {exc}", "sql": sql, **attempts_log}
        try:
            sql_fixed = enforce_limit(sanitize_sql(repaired["content"]))
            df = con.execute(sql_fixed).fetchdf()
            sql = sql_fixed
        except Exception as exc2:
            return {"error": f"SQL failed even after repair: {exc2}", "sql": sql, **attempts_log}
    finally:
        con.close()

    return {
        "sql": sql,
        "dataframe": df,
        "row_count": len(df),
        "provider": llm_result.get("provider"),
        "provider_label": llm_result.get("provider_label"),
        "model": llm_result.get("model"),
        "latency_ms": llm_result.get("latency_ms"),
        **attempts_log,
    }
