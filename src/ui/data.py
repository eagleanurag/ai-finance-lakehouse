"""Cached access to the gold warehouse for the Streamlit app.

One read-only DuckDB connection is shared per warehouse version (file mtime), query results are
cached, and the 2M-row fact table is pre-aggregated once into a small monthly cube so filters and
chart redraws never touch the fact table again.
"""
from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from config import settings


_pinned: float | None = None


def version() -> float:
    try:
        return settings.DUCKDB_PATH.stat().st_mtime
    except FileNotFoundError:
        return 0.0


def pin_version() -> float:
    """Read the warehouse version once per script run; every query in the run reuses it."""
    global _pinned
    _pinned = version()
    return _pinned


@st.cache_resource(max_entries=1, show_spinner=False)
def _connection(version_key: float) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)


def release() -> None:
    """Drop the shared connection (needed before the pipeline replaces the warehouse file)."""
    _connection.clear()
    st.cache_data.clear()


def query(sql: str, params: list | None = None) -> pd.DataFrame:
    cur = _connection(_pinned if _pinned is not None else version()).cursor()
    try:
        return cur.execute(sql, params or []).fetchdf()
    finally:
        cur.close()


@st.cache_data(ttl=1800, max_entries=512, show_spinner=False)
def cached(sql: str, version_key: float, params: tuple = ()) -> pd.DataFrame:
    return query(sql, list(params))


def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    return cached(sql, _pinned if _pinned is not None else version(), params)


@st.cache_data(ttl=1800, max_entries=4, show_spinner="Aggregating 2M transactions...")
def ledger_cube(version_key: float) -> pd.DataFrame:
    """month x department x account category/name x txn type — a few thousand rows."""
    df = query("""
        SELECT d.month_start, dep.dept_code, dep.department_name, dep.region, dep.business_function,
               a.category, a.account_name, f.txn_type,
               SUM(f.amount_usd)::DOUBLE AS amount, COUNT(*) AS txns
        FROM gold.fact_transactions f
        JOIN gold.dim_date d USING (date_sk)
        JOIN gold.dim_department dep USING (department_id)
        JOIN gold.dim_gl_account a USING (account_id)
        GROUP BY ALL
    """)
    df["month_start"] = pd.to_datetime(df["month_start"])
    return df


@st.cache_data(ttl=1800, max_entries=4, show_spinner=False)
def budget_cube(version_key: float) -> pd.DataFrame:
    df = query("""
        SELECT k.month_start, k.dept_code, k.department_name, d.region,
               k.revenue_actual::DOUBLE AS revenue_actual, k.expense_actual::DOUBLE AS expense_actual,
               k.budget_expense::DOUBLE AS budget_expense, k.budget_revenue::DOUBLE AS budget_revenue,
               k.is_month_to_date
        FROM gold.kpi_monthly_actual_vs_budget k JOIN gold.dim_department d USING (department_id)
    """)
    df["month_start"] = pd.to_datetime(df["month_start"])
    return df


@st.cache_data(ttl=1800, max_entries=4, show_spinner=False)
def customer_cube(version_key: float) -> pd.DataFrame:
    df = query("""
        SELECT d.month_start, dep.region AS dept_region, c.segment, c.region AS customer_region,
               SUM(f.amount_usd)::DOUBLE AS revenue, COUNT(DISTINCT f.customer_id) AS customers, COUNT(*) AS txns
        FROM gold.fact_transactions f
        JOIN gold.dim_date d USING (date_sk)
        JOIN gold.dim_department dep USING (department_id)
        JOIN gold.dim_gl_account a USING (account_id)
        JOIN gold.dim_customer c USING (customer_id)
        WHERE a.category = 'Revenue' AND f.customer_id > 0
        GROUP BY ALL
    """)
    df["month_start"] = pd.to_datetime(df["month_start"])
    return df


def table_exists(schema: str, table: str) -> bool:
    return not q("SELECT 1 FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?", (schema, table)).empty
