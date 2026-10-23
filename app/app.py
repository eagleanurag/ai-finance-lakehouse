from __future__ import annotations

import itertools
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="AI Finance Lakehouse",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from config import settings
from src.ai import agent as ai_agent
from src.ai.router import available_providers
from src.pipelines import bronze as bronze_api
from src.pipelines import ingest, snapshot, transform
from src.pipelines.sync import sync_all_warehouses
from src.sources.kafka_client import EventBroker
from src.ui import data
from src.ui import theme as T

T.register_template()
st.markdown(T.CSS, unsafe_allow_html=True)

PLOT_CONFIG = {"displayModeBar": False, "responsive": True}
_cards = itertools.count()


# --------------------------------------------------------------------------- bootstrap
def _warehouse_ready() -> bool:
    if not settings.DUCKDB_PATH.exists():
        return False
    try:
        con = duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)
        try:
            ok = con.execute("SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'gold' "
                             "AND table_name IN ('fact_transactions', 'fact_ap_invoices')").fetchall()[0][0] == 2
            return ok and con.execute("SELECT count(*) FROM gold.fact_transactions").fetchall()[0][0] > 0
        finally:
            con.close()
    except duckdb.Error:
        return False


@st.cache_resource(show_spinner="Loading the 2M-row warehouse snapshot published by the nightly pipeline...")
def ensure_warehouse() -> str:
    if _warehouse_ready():
        return "local"
    if snapshot.download():
        return "snapshot"
    with st.spinner("No snapshot published yet - generating sources and running the notebooks (a few minutes)..."):
        ingest.run_full_ingest(include_ocr=False)
        transform.run_transforms()
    return "built"


def load_meta(name: str) -> dict | None:
    path = settings.DATA_DIR / name
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


ensure_warehouse()
VERSION = data.pin_version()


@st.cache_resource(show_spinner=False)
def _warm_caches(version: float) -> bool:
    import threading

    from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

    def run() -> None:
        try:
            data.ledger_cube(version)
            data.budget_cube(version)
            data.customer_cube(version)
            # pull the columns the other views scan into DuckDB's buffer pool before anyone clicks
            data.query("SELECT count(DISTINCT vendor_id), count(DISTINCT customer_id), sum(amount_usd), "
                       "max(txn_type), max(event_ts), max(source_channel) FROM gold.fact_transactions")
            data.query("SELECT sum(open_amount_usd), sum(amount_usd), sum(paid_amount_usd), max(paid_date), "
                       "max(aging_bucket), max(status), max(days_past_due), max(vendor_id) FROM gold.fact_ap_invoices")
            data.query("SELECT max(vendor_name), max(customer_name) FROM gold.dim_vendor, gold.dim_customer LIMIT 1")
        except Exception:
            pass

    thread = threading.Thread(target=run, daemon=True)
    add_script_run_ctx(thread, get_script_run_ctx())
    thread.start()
    return True



# --------------------------------------------------------------------------- ui helpers
def card():
    return st.container(key=f"card_{next(_cards)}")


def plot(fig: go.Figure, height: int = 340) -> None:
    fig.update_layout(height=height, legend_title_text="")
    st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG)


def hero(eyebrow: str, title_html: str, text: str) -> None:
    st.markdown(f'<div class="hero"><div class="eyebrow">{eyebrow}</div><h1>{title_html}</h1><p>{text}</p></div>',
                unsafe_allow_html=True)


def money_axis(fig: go.Figure, axis: str = "y") -> None:
    fig.update_layout(**{f"{axis}axis": dict(tickprefix="$", exponentformat="B", separatethousands=True)})


def pad_x(fig: go.Figure, values) -> None:
    """Leave room for outside value labels on horizontal bars."""
    lo, hi = min(0.0, float(min(values))), max(0.0, float(max(values)))
    span = (hi - lo) or 1.0
    fig.update_xaxes(range=[lo - (0.28 * span if lo < 0 else 0), hi + 0.22 * span])


def _money_treemap(fig: go.Figure) -> None:
    trace = fig.data[0]
    labels = [T.money(v) for v in trace.values]
    trace.update(customdata=[[m] for m in labels], texttemplate="%{label}<br>%{customdata[0]}",
                 hovertemplate="%{label}<br>%{customdata[0]}<extra></extra>", root_color=T.SURFACE)


def months_between(start: pd.Timestamp, end: pd.Timestamp) -> int:
    return (end.year - start.year) * 12 + end.month - start.month + 1


def date_sk(ts: pd.Timestamp, month_end: bool = False) -> int:
    if month_end:
        ts = ts + pd.offsets.MonthEnd(0)
    return int(ts.strftime("%Y%m%d"))


# --------------------------------------------------------------------------- sidebar
with st.sidebar:
    st.markdown(
        '<div style="display:flex;align-items:center;gap:10px;margin:4px 0 2px">'
        '<div style="width:36px;height:36px;border-radius:10px;display:grid;place-items:center;font-size:18px;'
        'background:linear-gradient(135deg,#3987e5,#9085e9);box-shadow:0 0 18px rgba(144,133,233,.45)">📊</div>'
        '<div><div style="font:700 1.05rem Space Grotesk,Inter;color:#f0efec">AI Finance Lakehouse</div>'
        '<div style="font-size:.74rem;color:#898781">Medallion · 2M rows · GenAI analyst</div></div></div>',
        unsafe_allow_html=True,
    )
    st.write("")
    page = st.radio(
        "Navigate",
        ["🏠 Overview", "📈 Dashboard", "🤖 AI Analyst", "🧪 Data Quality", "🔍 Data Explorer", "🔄 Pipeline Ops"],
        index=1,
        label_visibility="collapsed",
    )
    st.divider()
    providers = available_providers()
    provider_options = ["Auto (Smart Fallback)"] + [p["label"] for p in providers]
    provider_map = {p["label"]: p["key"] for p in providers}
    selected_label = st.selectbox("AI model", provider_options, index=0, disabled=not providers)
    provider_key = "auto" if selected_label.startswith("Auto") else provider_map[selected_label]

    facts = data.q("SELECT count(*) AS n FROM gold.fact_transactions")["n"].iloc[0]
    meta = load_meta("transform_meta.json") or {}
    ts = str(meta.get("last_transform_utc", ""))[:16].replace("T", " ")
    st.markdown(
        f'<div style="margin-top:6px">'
        f'<span class="chip"><span class="dot"></span>{T.count(facts)} fact rows</span>'
        f'<span class="chip">⚡ {EventBroker().mode.replace("_", " ")}</span>'
        f'<span class="chip">🤖 {len(providers)} LLM provider{"s" if len(providers) != 1 else ""}</span>'
        + (f'<span class="chip">🕒 {ts} UTC</span>' if ts else "") + "</div>",
        unsafe_allow_html=True,
    )
    if st.button("♻️ Refresh data", width="stretch"):
        data.release()
        st.rerun()


# --------------------------------------------------------------------------- overview
def render_overview() -> None:
    hero("Enterprise data platform", 'Streaming lakehouse + <span class="grad">GenAI financial analyst</span>',
         "Eight raw sources with deliberately messy data land in bronze, two notebooks clean and model them into a "
         "tested star schema, and the result powers this dashboard, Power BI and an LLM analyst — all on free tiers.")

    has_audit = data.table_exists("audit", "dq_results")
    stats = data.q("""
        SELECT (SELECT count(*) FROM gold.fact_transactions) AS facts,
               (SELECT count(DISTINCT date_trunc('month', event_ts)) FROM gold.fact_transactions) AS months,
               (SELECT count(*) FROM gold.dim_customer WHERE customer_id > 0) AS customers,
               (SELECT count(*) FROM gold.dim_vendor WHERE vendor_id > 0) AS vendors,
               (SELECT count(*) FROM gold.fact_ap_invoices) AS invoices,
               (SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'gold') AS gold_tables
    """).iloc[0]
    dq = data.q("""
        SELECT sum(failed_rows) FILTER (WHERE action = 'quarantined') AS quarantined,
               count(*) FILTER (WHERE layer = 'gold') AS tests,
               count(*) FILTER (WHERE layer = 'gold' AND severity = 'error' AND failed_rows > 0) AS failed
        FROM audit.dq_results WHERE run_id = (SELECT max(run_id) FROM audit.dq_results)
    """).iloc[0] if has_audit else None

    cols = st.columns(6)
    cols[0].metric("Fact rows", T.count(stats["facts"]), border=True)
    cols[1].metric("Months", int(stats["months"]), border=True)
    cols[2].metric("Customers", T.count(stats["customers"]), border=True)
    cols[3].metric("Vendors", T.count(stats["vendors"]), border=True)
    cols[4].metric("AP invoices", T.count(stats["invoices"]), border=True)
    cols[5].metric("Gold tests",
                   f"{int(dq['tests'] - dq['failed'])}/{int(dq['tests'])}" if dq is not None else "—", border=True)

    st.markdown("#### How data flows")
    steps = [
        (T.BLUE, "8 sources", "ERP · POS · Kafka · budgets · AP · FX · SharePoint · CRM"),
        (T.YELLOW, "Bronze", "Raw parquet / CSV / JSONL exactly as delivered"),
        (T.INK_2, "Silver notebook", "CDC, dedupe, FX, SCD2, quarantine"
         + (f" ({T.count(dq['quarantined'])} rows)" if dq is not None else "")),
        (T.AQUA, "Gold notebook", f"Star schema · {int(stats['gold_tables'])} tables · tests gate publish"),
        (T.VIOLET, "Serving", "This app · Power BI · AI analyst · 4 cloud warehouses"),
    ]
    st.markdown('<div class="flow">' + "".join(
        f'<div class="step"><i style="background:{c}"></i><b>{t}</b><span>{d}</span></div>' for c, t, d in steps
    ) + "</div>", unsafe_allow_html=True)
    st.write("")

    left, right = st.columns([3, 2])
    with left, card():
        mix = data.q("""
            SELECT date_trunc('month', event_ts)::DATE AS month, source_channel, count(*) AS rows
            FROM gold.fact_transactions GROUP BY ALL ORDER BY 1
        """)
        fig = px.bar(mix, x="month", y="rows", color="source_channel", color_discrete_map=T.CHANNEL_COLORS,
                     title="Ledger rows by ingestion channel")
        fig.update_traces(hovertemplate="%{x|%b %Y}<br>%{fullData.name}: %{y:,}<extra></extra>")
        fig.update_layout(barmode="stack", xaxis_title=None, yaxis_title=None)
        plot(fig, 330)
    with right, card():
        sizes = data.q("""
            SELECT table_name, estimated_size AS rows FROM duckdb_tables()
            WHERE schema_name = 'gold' ORDER BY rows DESC
        """)
        sizes["kind"] = sizes["table_name"].str.split("_").str[0].map({"fact": "Fact", "dim": "Dimension"}).fillna("KPI")
        fig = px.bar(sizes, x="rows", y="table_name", orientation="h", log_x=True, color="kind",
                     color_discrete_map={"Fact": T.BLUE, "Dimension": T.VIOLET, "KPI": T.YELLOW},
                     title="Gold tables (rows, log scale)")
        fig.update_traces(hovertemplate="%{y}: %{x:,} rows<extra></extra>")
        fig.update_layout(yaxis=dict(autorange="reversed", title=None), xaxis_title=None)
        plot(fig, 330)

    with st.expander("Stack & free tiers"):
        st.markdown(
            "| Layer | Tech | Free tier |\n|---|---|---|\n"
            "| Streaming | Aiven Apache Kafka | Forever free |\n"
            "| Bronze lake | Local dir → Azure Blob | 5 GB always-free blob |\n"
            "| Transform | Jupyter notebooks on DuckDB | Runs in CI and in this app |\n"
            "| Warehouses | DuckDB · MotherDuck · BigQuery · Neon | Free plans |\n"
            "| Orchestration | GitHub Actions cron + release snapshot | 2,000 min/mo |\n"
            "| App host | Streamlit Community Cloud | Forever free |\n"
            "| LLMs | Groq / Gemini / Mistral / OpenRouter | Free dev tiers |"
        )


# --------------------------------------------------------------------------- dashboard
PERIODS = ["Last 12 months", "Year to date", "Last 24 months", "All history"]


def _period_bounds(period: str, last: pd.Timestamp, first: pd.Timestamp):
    if period == "Last 12 months":
        start = last - pd.DateOffset(months=11)
    elif period == "Year to date":
        start = pd.Timestamp(last.year, 1, 1)
    elif period == "Last 24 months":
        start = last - pd.DateOffset(months=23)
    else:
        start = first
    start = max(start, first)
    n = months_between(start, last)
    prev_end = start - pd.DateOffset(months=1)
    prev_start = prev_end - pd.DateOffset(months=n - 1)
    has_prev = prev_start >= first and period != "All history"
    return start, (prev_start, prev_end) if has_prev else None


def render_dashboard() -> None:
    cube = data.ledger_cube(VERSION)
    first, last = cube["month_start"].min(), cube["month_start"].max()

    top_l, top_r = st.columns([3, 2], vertical_alignment="bottom")
    with top_l:
        hero("Executive finance dashboard", 'Performance <span class="grad">at a glance</span>',
             f"{T.count(cube['txns'].sum())} posted transactions · {first:%b %Y} – {last:%b %Y} · "
             f"{last:%B} is month-to-date with a prorated budget.")
    with top_r:
        f1, f2 = st.columns([3, 2])
        period = f1.segmented_control("Period", PERIODS, default="Last 12 months", required=True, key="period")
        region = f2.segmented_control("Region", ["All", "NA", "EMEA"], default="All", required=True, key="region")

    start, prev = _period_bounds(period, last, first)
    regional = cube if region == "All" else cube[cube["region"] == region]
    cur = regional[regional["month_start"] >= start]
    old = regional[regional["month_start"].between(*prev)] if prev else None
    bud = data.budget_cube(VERSION)
    bud = bud if region == "All" else bud[bud["region"] == region]
    cur_bud = bud[bud["month_start"] >= start]
    old_bud = bud[bud["month_start"].between(*prev)] if prev else None

    def pnl(df: pd.DataFrame | None) -> dict:
        if df is None or df.empty:
            return {}
        rev = df.loc[df["category"] == "Revenue", "amount"].sum()
        exp = df.loc[df["category"] != "Revenue", "amount"].sum()
        rev_txn = df.loc[df["category"] == "Revenue", "txns"].sum()
        return {"rev": rev, "exp": exp, "net": rev - exp, "margin": 100 * (rev - exp) / rev if rev else None,
                "txns": df["txns"].sum(), "ticket": rev / rev_txn if rev_txn else None}

    def variance(b: pd.DataFrame | None) -> float | None:
        if b is None or b.empty or not b["budget_expense"].sum():
            return None
        return 100 * (b["expense_actual"].sum() / b["budget_expense"].sum() - 1)

    now, before = pnl(cur), pnl(old)
    var_now, var_before = variance(cur_bud), variance(old_bud)
    monthly = cur.assign(is_rev=cur["category"] == "Revenue").groupby(["month_start", "is_rev"])["amount"].sum().unstack(fill_value=0)
    monthly.columns = ["expense" if not c else "revenue" for c in monthly.columns]
    monthly["net"] = monthly["revenue"] - monthly["expense"]
    monthly["margin"] = 100 * monthly["net"] / monthly["revenue"].where(monthly["revenue"] != 0)
    lo, hi = date_sk(start), date_sk(last, month_end=True)
    reg = "" if region == "All" else "AND dep.region = ?"
    reg_p: tuple = () if region == "All" else (region,)

    def active_customers(a: int, b: int) -> int:
        return int(data.q(f"""
            SELECT count(DISTINCT f.customer_id) AS n FROM gold.fact_transactions f
            JOIN gold.dim_department dep USING (department_id)
            WHERE f.customer_id > 0 AND f.date_sk BETWEEN ? AND ? {reg}""", (a, b) + reg_p)["n"].iloc[0])

    cust_now = active_customers(lo, hi)
    cust_before = active_customers(date_sk(prev[0]), date_sk(prev[1], True)) if prev else None

    def delta(key):
        return T.pct_delta(now.get(key), before.get(key)) if before else None

    k = st.columns(4)
    k[0].metric("Revenue", T.money(now["rev"]), delta("rev"), border=True,
                chart_data=monthly["revenue"].round().tolist(), chart_type="area")
    k[1].metric("Operating costs", T.money(now["exp"]), delta("exp"), delta_color="inverse", border=True,
                chart_data=monthly["expense"].round().tolist(), chart_type="area")
    k[2].metric("Net profit", T.money(now["net"]), delta("net"), border=True,
                chart_data=monthly["net"].round().tolist(), chart_type="bar")
    k[3].metric("Net margin", f"{now['margin']:.1f}%",
                f"{now['margin'] - before['margin']:+.1f} pp" if before else None, border=True,
                chart_data=monthly["margin"].round(1).fillna(0).tolist(), chart_type="line")
    k = st.columns(4)
    k[0].metric("Spend vs budget", f"{var_now:+.1f}%" if var_now is not None else "—",
                f"{var_now - var_before:+.1f} pp" if var_now is not None and var_before is not None else None,
                delta_color="inverse", border=True, help="Actual costs vs the latest plan version (prorated MTD)")
    k[1].metric("Active customers", T.count(cust_now), T.pct_delta(cust_now, cust_before), border=True)
    k[2].metric("Transactions", T.count(now["txns"]), delta("txns"), border=True)
    k[3].metric("Avg revenue ticket", T.money(now["ticket"], 2), delta("ticket"), border=True)

    section = st.segmented_control(
        "View", ["Executive", "Revenue", "Spend & budget", "Payables", "Departments"],
        default="Executive", required=True, key="dash_section", label_visibility="collapsed")
    st.write("")

    if section == "Executive":
        _exec_view(cur, monthly)
    elif section == "Revenue":
        _revenue_view(start, last, region, lo, hi, reg, reg_p)
    elif section == "Spend & budget":
        _spend_view(cur, cur_bud, lo, hi, reg, reg_p)
    elif section == "Payables":
        _payables_view()
    else:
        _departments_view(cur, cur_bud)

    _powerbi_block()


def _exec_view(cur: pd.DataFrame, monthly: pd.DataFrame) -> None:
    left, right = st.columns([3, 2])
    with left, card():
        m = monthly.reset_index()
        fig = go.Figure([
            go.Bar(x=m["month_start"], y=m["revenue"], name="Revenue", marker_color=T.BLUE,
                   hovertemplate="%{x|%b %Y}<br>Revenue %{y:$,.0f}<extra></extra>"),
            go.Bar(x=m["month_start"], y=m["expense"], name="Costs", marker_color=T.ORANGE,
                   hovertemplate="%{x|%b %Y}<br>Costs %{y:$,.0f}<extra></extra>"),
        ])
        fig.update_layout(title="Revenue vs operating costs by month", barmode="group", xaxis_title=None)
        money_axis(fig)
        plot(fig, 340)
    with right, card():
        rev = cur.loc[cur["category"] == "Revenue", "amount"].sum()
        cats = [c for c in ["COGS", "OPEX", "CAPEX", "Other", "Unclassified"] if c in set(cur["category"])]
        costs = [cur.loc[cur["category"] == c, "amount"].sum() for c in cats]
        fig = go.Figure(go.Waterfall(
            x=["Revenue"] + cats + ["Net profit"], measure=["relative"] * (len(cats) + 1) + ["total"],
            y=[rev] + [-c for c in costs] + [0], text=[T.money(v) for v in [rev] + [-c for c in costs] + [rev - sum(costs)]],
            textposition="outside", textfont=dict(color=T.INK_2, size=11),
            increasing=dict(marker=dict(color=T.BLUE)), decreasing=dict(marker=dict(color=T.RED)),
            totals=dict(marker=dict(color=T.VIOLET)), connector=dict(line=dict(color=T.AXIS, width=1)),
            hovertemplate="%{x}: %{y:$,.0f}<extra></extra>"))
        fig.update_layout(title="Profit & loss bridge", showlegend=False)
        money_axis(fig)
        plot(fig, 340)

    a, b, c = st.columns([2, 2, 3])
    with a, card():
        m = monthly.reset_index()
        fig = go.Figure(go.Scatter(x=m["month_start"], y=m["margin"], mode="lines+markers", line=dict(color=T.AQUA),
                                   fill="tozeroy", fillcolor="rgba(25,158,112,.12)",
                                   hovertemplate="%{x|%b %Y}<br>Net margin %{y:.1f}%<extra></extra>"))
        fig.update_layout(title="Net margin %", yaxis=dict(ticksuffix="%", rangemode="tozero"), xaxis_title=None)
        plot(fig, 300)
    with b, card():
        spend = cur[cur["category"] != "Revenue"].groupby("category", as_index=False)["amount"].sum()
        fig = px.pie(spend, names="category", values="amount", hole=0.62, color="category",
                     color_discrete_map=T.CATEGORY_COLORS, title="Cost mix by GL category")
        fig.update_traces(textinfo="percent", textfont=dict(color="#ffffff"),
                          hovertemplate="%{label}: %{value:$,.0f} (%{percent})<extra></extra>")
        plot(fig, 300)
    with c, card():
        dept = cur[cur["category"] != "Revenue"].groupby(["region", "department_name"], as_index=False)["amount"].sum()
        fig = px.treemap(dept, path=[px.Constant("All departments"), "region", "department_name"], values="amount",
                         color="region", color_discrete_map={**T.REGION_COLORS, "(?)": T.SURFACE},
                         title="Where the money goes (costs by department)")
        _money_treemap(fig)
        plot(fig, 300)


def _revenue_view(start, last, region, lo, hi, reg, reg_p) -> None:
    cc = data.customer_cube(VERSION)
    cc = cc[cc["month_start"] >= start]
    if region != "All":
        cc = cc[cc["dept_region"] == region]
    left, right = st.columns([3, 2])
    with left, card():
        seg = cc.groupby(["month_start", "segment"], as_index=False)["revenue"].sum()
        fig = px.area(seg, x="month_start", y="revenue", color="segment", color_discrete_map=T.SEGMENT_COLORS,
                      category_orders={"segment": list(T.SEGMENT_COLORS)}, title="Revenue by customer segment")
        fig.update_traces(hovertemplate="%{x|%b %Y}<br>%{fullData.name}: %{y:$,.0f}<extra></extra>", line=dict(width=2))
        fig.update_layout(xaxis_title=None, yaxis_title=None)
        money_axis(fig)
        plot(fig, 340)
    with right, card():
        creg = cc.groupby("customer_region", as_index=False)["revenue"].sum()
        fig = px.pie(creg, names="customer_region", values="revenue", hole=0.62, color="customer_region",
                     color_discrete_map=T.REGION_COLORS, title="Revenue by customer region")
        fig.update_traces(textinfo="percent+label", textfont=dict(color="#ffffff"),
                          hovertemplate="%{label}: %{value:$,.0f}<extra></extra>")
        fig.update_layout(showlegend=False)
        plot(fig, 340)

    a, b = st.columns(2)
    with a, card():
        top = data.q(f"""
            SELECT c.customer_name, c.segment, sum(f.amount_usd)::DOUBLE AS revenue, count(*) AS orders
            FROM gold.fact_transactions f
            JOIN gold.dim_customer c USING (customer_id)
            JOIN gold.dim_department dep USING (department_id)
            WHERE f.customer_id > 0 AND f.txn_type <> 'expense' AND f.date_sk BETWEEN ? AND ? {reg}
            GROUP BY ALL ORDER BY revenue DESC LIMIT 10""", (lo, hi) + reg_p)
        fig = px.bar(top, x="revenue", y="customer_name", orientation="h", color="segment",
                     color_discrete_map=T.SEGMENT_COLORS, text=top["revenue"].map(T.money), title="Top 10 customers")
        fig.update_traces(textposition="outside", textfont=dict(color=T.INK_2), cliponaxis=False,
                          hovertemplate="%{y}<br>%{x:$,.0f}<extra></extra>")
        fig.update_layout(yaxis=dict(categoryorder="total ascending", title=None), xaxis_title=None)
        money_axis(fig, "x")
        pad_x(fig, top["revenue"])
        plot(fig, 380)
    with b, card():
        prod = data.q(f"""
            SELECT a.account_name AS product, sum(f.amount_usd)::DOUBLE AS revenue
            FROM gold.fact_transactions f
            JOIN gold.dim_gl_account a USING (account_id)
            JOIN gold.dim_department dep USING (department_id)
            WHERE a.category = 'Revenue' AND f.date_sk BETWEEN ? AND ? {reg}
            GROUP BY 1 ORDER BY 2 DESC""", (lo, hi) + reg_p)
        fig = px.bar(prod, x="revenue", y="product", orientation="h", text=prod["revenue"].map(T.money),
                     title="Revenue by product line")
        fig.update_traces(marker_color=T.BLUE, textposition="outside", textfont=dict(color=T.INK_2), cliponaxis=False,
                          hovertemplate="%{y}<br>%{x:$,.0f}<extra></extra>")
        fig.update_layout(yaxis=dict(autorange="reversed", title=None), xaxis_title=None)
        money_axis(fig, "x")
        pad_x(fig, prod["revenue"])
        plot(fig, 380)

    with card():
        wd = data.q(f"""
            SELECT d.day_name, d.day_of_week, sum(f.amount_usd)::DOUBLE AS revenue, count(*) AS orders
            FROM gold.fact_transactions f
            JOIN gold.dim_date d USING (date_sk)
            JOIN gold.dim_gl_account a USING (account_id)
            JOIN gold.dim_department dep USING (department_id)
            WHERE a.category = 'Revenue' AND f.date_sk BETWEEN ? AND ? {reg}
            GROUP BY ALL""", (lo, hi) + reg_p)
        wd["order"] = (wd["day_of_week"] + 6) % 7
        wd = wd.sort_values("order")
        fig = px.bar(wd, x="day_name", y="revenue", text=wd["revenue"].map(T.money),
                     title="Revenue by weekday — month-end and weekday effects")
        fig.update_traces(marker_color=T.VIOLET, textposition="outside", textfont=dict(color=T.INK_2), cliponaxis=False,
                          hovertemplate="%{x}: %{y:$,.0f}<extra></extra>")
        fig.update_layout(xaxis_title=None, yaxis_title=None)
        money_axis(fig)
        plot(fig, 280)


def _spend_view(cur, cur_bud, lo, hi, reg, reg_p) -> None:
    left, right = st.columns([3, 2])
    with left, card():
        m = cur_bud.groupby("month_start", as_index=False)[["expense_actual", "budget_expense"]].sum()
        fig = go.Figure([
            go.Bar(x=m["month_start"], y=m["expense_actual"], name="Actual costs", marker_color=T.ORANGE,
                   hovertemplate="%{x|%b %Y}<br>Actual %{y:$,.0f}<extra></extra>"),
            go.Scatter(x=m["month_start"], y=m["budget_expense"], name="Budget (latest plan)", mode="lines+markers",
                       line=dict(color=T.INK_2, dash="dot"), marker=dict(size=8, color=T.INK_2),
                       hovertemplate="%{x|%b %Y}<br>Budget %{y:$,.0f}<extra></extra>"),
        ])
        fig.update_layout(title="Costs vs budget by month", xaxis_title=None)
        money_axis(fig)
        plot(fig, 340)
    with right, card():
        d = cur_bud.groupby("department_name", as_index=False)[["expense_actual", "budget_expense"]].sum()
        d = d[(d["budget_expense"] > 0) & (d["department_name"] != "Unknown department")]
        d["var"] = 100 * (d["expense_actual"] / d["budget_expense"] - 1)
        d = d.reindex(d["var"].abs().sort_values(ascending=False).index).head(14).sort_values("var")
        fig = go.Figure(go.Bar(
            x=d["var"], y=d["department_name"], orientation="h",
            marker_color=[T.DIVERGING["neg"] if v > 0 else T.DIVERGING["pos"] for v in d["var"]],
            text=[f"{v:+.1f}%" for v in d["var"]], textposition="outside", textfont=dict(color=T.INK_2),
            cliponaxis=False, hovertemplate="%{y}<br>%{x:+.1f}% vs budget<extra></extra>"))
        fig.update_layout(title="Budget variance by department (red = over)", xaxis=dict(ticksuffix="%", zeroline=True,
                          zerolinecolor=T.AXIS), yaxis_title=None)
        pad_x(fig, d["var"])
        plot(fig, 340)

    a, b = st.columns([3, 2])
    with a, card():
        acc = cur[cur["category"] != "Revenue"].groupby(["category", "account_name"], as_index=False)["amount"].sum()
        fig = px.treemap(acc, path=[px.Constant("All costs"), "category", "account_name"], values="amount",
                         color="category", color_discrete_map={**T.CATEGORY_COLORS, "(?)": T.SURFACE},
                         title="Costs by GL category and account")
        _money_treemap(fig)
        plot(fig, 400)
    with b, card():
        ven = data.q(f"""
            SELECT v.vendor_name, v.vendor_category, sum(f.amount_usd)::DOUBLE AS spend
            FROM gold.fact_transactions f
            JOIN gold.dim_vendor v USING (vendor_id)
            JOIN gold.dim_department dep USING (department_id)
            WHERE f.vendor_id > 0 AND f.date_sk BETWEEN ? AND ? {reg}
            GROUP BY ALL ORDER BY spend DESC LIMIT 10""", (lo, hi) + reg_p)
        fig = px.bar(ven, x="spend", y="vendor_name", orientation="h", text=ven["spend"].map(T.money),
                     title="Top 10 suppliers by spend", hover_data={"vendor_category": True})
        fig.update_traces(marker_color=T.ORANGE, textposition="outside", textfont=dict(color=T.INK_2), cliponaxis=False)
        fig.update_layout(yaxis=dict(autorange="reversed", title=None), xaxis_title=None)
        money_axis(fig, "x")
        pad_x(fig, ven["spend"])
        plot(fig, 400)


def _payables_view() -> None:
    st.markdown('<div class="section-note">Accounts payable is a point-in-time view as of the last pipeline run '
                '(not affected by the period and region filters).</div>', unsafe_allow_html=True)
    s = data.q("""
        SELECT sum(open_amount_usd)::DOUBLE AS open_usd,
               sum(open_amount_usd) FILTER (WHERE days_past_due > 0)::DOUBLE AS overdue_usd,
               count(*) FILTER (WHERE open_amount_usd <> 0) AS open_invoices,
               avg(days_past_due) FILTER (WHERE days_past_due > 0) AS avg_dpd,
               avg(CASE WHEN f.status = 'PAID' THEN (p.full_date <= d.full_date)::INT END) * 100 AS on_time_pct
        FROM gold.fact_ap_invoices f
        JOIN gold.dim_date d ON d.date_sk = f.due_date_sk
        LEFT JOIN gold.dim_date p ON p.full_date = f.paid_date
    """).iloc[0]
    k = st.columns(5)
    k[0].metric("Open payables", T.money(s["open_usd"]), border=True)
    k[1].metric("Overdue", T.money(s["overdue_usd"]), border=True)
    k[2].metric("Overdue share", f"{100 * (s['overdue_usd'] or 0) / s['open_usd']:.1f}%" if s["open_usd"] else "—", border=True)
    k[3].metric("Avg days past due", f"{s['avg_dpd']:.0f}" if pd.notna(s["avg_dpd"]) else "—", border=True)
    k[4].metric("Paid on time", f"{s['on_time_pct']:.1f}%" if pd.notna(s["on_time_pct"]) else "—", border=True)

    left, right = st.columns(2)
    with left, card():
        ag = data.q("""
            SELECT aging_bucket, count(*) AS invoices, sum(open_amount_usd)::DOUBLE AS open_usd
            FROM gold.fact_ap_invoices WHERE aging_bucket <> 'Closed' GROUP BY 1""")
        fig = px.bar(ag, x="aging_bucket", y="open_usd", color="aging_bucket", color_discrete_map=T.AGING_COLORS,
                     category_orders={"aging_bucket": T.AGING_ORDER}, text=ag["open_usd"].map(T.money),
                     title="Open payables by aging bucket", custom_data=["invoices"])
        fig.update_traces(textposition="outside", textfont=dict(color=T.INK_2), cliponaxis=False,
                          hovertemplate="%{x} days<br>%{y:$,.0f} · %{customdata[0]:,} invoices<extra></extra>")
        fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None)
        money_axis(fig)
        plot(fig, 340)
    with right, card():
        bycat = data.q("""
            SELECT v.vendor_category, f.aging_bucket, sum(f.open_amount_usd)::DOUBLE AS open_usd
            FROM gold.fact_ap_invoices f JOIN gold.dim_vendor v USING (vendor_id)
            WHERE f.aging_bucket <> 'Closed' GROUP BY ALL""")
        order = bycat.groupby("vendor_category")["open_usd"].sum().sort_values().index.tolist()
        fig = px.bar(bycat, x="open_usd", y="vendor_category", color="aging_bucket", orientation="h",
                     color_discrete_map=T.AGING_COLORS,
                     category_orders={"aging_bucket": T.AGING_ORDER, "vendor_category": order[::-1]},
                     title="Open payables by supplier category")
        fig.update_traces(hovertemplate="%{y} · %{fullData.name}<br>%{x:$,.0f}<extra></extra>")
        fig.update_layout(barmode="stack", yaxis_title=None, xaxis_title=None)
        money_axis(fig, "x")
        plot(fig, 340)

    a, b = st.columns([3, 2])
    with a, card():
        trend = data.q("""
            SELECT d.month_start, sum(f.amount_usd)::DOUBLE AS invoiced, sum(f.paid_amount_usd)::DOUBLE AS paid
            FROM gold.fact_ap_invoices f JOIN gold.dim_date d ON d.date_sk = f.invoice_date_sk
            WHERE f.status <> 'VOID' GROUP BY 1 ORDER BY 1""")
        fig = go.Figure([
            go.Bar(x=trend["month_start"], y=trend["invoiced"], name="Invoiced", marker_color=T.BLUE,
                   hovertemplate="%{x|%b %Y}<br>Invoiced %{y:$,.0f}<extra></extra>"),
            go.Bar(x=trend["month_start"], y=trend["paid"], name="Paid", marker_color=T.AQUA,
                   hovertemplate="%{x|%b %Y}<br>Paid %{y:$,.0f}<extra></extra>"),
        ])
        fig.update_layout(title="Invoiced vs paid by invoice month", barmode="group", xaxis_title=None)
        money_axis(fig)
        plot(fig, 340)
    with b, card():
        late = data.q("""
            SELECT v.vendor_name, count(*) AS invoices, sum(f.open_amount_usd)::DOUBLE AS overdue_usd,
                   max(f.days_past_due) AS max_days
            FROM gold.fact_ap_invoices f JOIN gold.dim_vendor v USING (vendor_id)
            WHERE f.days_past_due > 0 GROUP BY 1 ORDER BY overdue_usd DESC LIMIT 12""")
        st.markdown("**Most overdue suppliers**")
        st.dataframe(late, hide_index=True, width="stretch", height=300, column_config={
            "vendor_name": "Supplier",
            "invoices": st.column_config.NumberColumn("Invoices"),
            "overdue_usd": st.column_config.ProgressColumn("Overdue $", format="compact", min_value=0,
                                                           max_value=float(late["overdue_usd"].max() or 1)),
            "max_days": st.column_config.NumberColumn("Max days late"),
        })


def _departments_view(cur: pd.DataFrame, cur_bud: pd.DataFrame) -> None:
    heads = data.q("SELECT dept_code, headcount, cost_center, is_active FROM gold.dim_department WHERE department_id > 0")
    g = cur_bud.groupby(["dept_code", "department_name", "region"], as_index=False)[
        ["revenue_actual", "expense_actual", "budget_expense"]].sum()
    spark = (cur_bud.sort_values("month_start").groupby("dept_code")["expense_actual"]
             .apply(lambda s: s.round().tolist()).rename("trend"))
    g = g.merge(heads, on="dept_code", how="left").merge(spark, on="dept_code", how="left")
    g["used"] = 100 * g["expense_actual"] / g["budget_expense"].where(g["budget_expense"] > 0)
    g["per_head"] = g["expense_actual"] / g["headcount"].where(g["headcount"] > 0)
    g = g.sort_values("expense_actual", ascending=False)

    with card():
        st.markdown("**Department scorecard**")
        st.dataframe(
            g[["department_name", "region", "headcount", "revenue_actual", "expense_actual", "budget_expense",
               "used", "per_head", "trend"]],
            hide_index=True, width="stretch", height=430,
            column_config={
                "department_name": "Department", "region": "Region",
                "headcount": st.column_config.NumberColumn("Headcount"),
                "revenue_actual": st.column_config.NumberColumn("Revenue $", format="compact"),
                "expense_actual": st.column_config.NumberColumn("Costs $", format="compact"),
                "budget_expense": st.column_config.NumberColumn("Budget $", format="compact"),
                "used": st.column_config.ProgressColumn("Budget used", format="%.0f%%", min_value=0, max_value=130),
                "per_head": st.column_config.NumberColumn("Cost / head $", format="compact"),
                "trend": st.column_config.AreaChartColumn("Monthly costs", y_min=0),
            },
        )
    left, right = st.columns(2)
    with left, card():
        top = g.dropna(subset=["per_head"]).nlargest(12, "per_head").sort_values("per_head")
        fig = px.bar(top, x="per_head", y="department_name", orientation="h", color="region",
                     color_discrete_map=T.REGION_COLORS, text=top["per_head"].map(T.money),
                     title="Cost per employee (highest 12)")
        fig.update_traces(textposition="outside", textfont=dict(color=T.INK_2), cliponaxis=False,
                          hovertemplate="%{y}<br>%{x:$,.0f} per head<extra></extra>")
        fig.update_layout(yaxis=dict(categoryorder="total ascending", title=None), xaxis_title=None)
        money_axis(fig, "x")
        pad_x(fig, top["per_head"])
        plot(fig, 400)
    with right, card():
        fn = cur[cur["category"] != "Revenue"].groupby(["business_function", "region"], as_index=False)["amount"].sum()
        order = fn.groupby("business_function")["amount"].sum().sort_values(ascending=False).index.tolist()
        fig = px.bar(fn, x="business_function", y="amount", color="region", color_discrete_map=T.REGION_COLORS,
                     category_orders={"business_function": order}, title="Costs by business function")
        fig.update_traces(hovertemplate="%{x} · %{fullData.name}<br>%{y:$,.0f}<extra></extra>")
        fig.update_layout(barmode="stack", xaxis_title=None, yaxis_title=None)
        money_axis(fig)
        plot(fig, 400)


def _powerbi_block() -> None:
    embed_url = settings.POWERBI_EMBED_URL
    with st.expander("📊 Power BI report", expanded=False):
        user_url = st.text_input("Publish-to-web URL", value=embed_url, placeholder="https://app.powerbi.com/view?r=...",
                                 help="Power BI Service: File → Embed report → Publish to web. Or set POWERBI_EMBED_URL.")
        final_url = (user_url or "").strip()
        if final_url.startswith("http"):
            st.link_button("↗ Open full report", final_url)
            components.iframe(final_url, height=620, scrolling=True)
        else:
            st.caption("No Power BI report connected yet — every KPI above is rendered natively from the gold layer.")


# --------------------------------------------------------------------------- AI analyst
INSIGHTS = {
    "📈 Revenue, costs & profit by month": ("""
        SELECT month_start, total_revenue AS revenue, total_expense AS costs, net_profit
        FROM gold.monthly_financials ORDER BY month_start""", "line"),
    "🏢 Top 10 departments by spend (last 12 months)": ("""
        SELECT dept_code, department_name, round(sum(expense_actual)) AS spend
        FROM gold.kpi_monthly_actual_vs_budget
        WHERE month_start > (SELECT max(month_start) - INTERVAL 12 MONTH FROM gold.kpi_monthly_actual_vs_budget)
        GROUP BY 1, 2 ORDER BY spend DESC LIMIT 10""", "bar"),
    "⚠️ Departments over budget last full month": ("""
        SELECT dept_code, department_name, round(expense_actual) AS spend, round(budget_expense) AS budget,
               expense_variance_pct
        FROM gold.kpi_monthly_actual_vs_budget
        WHERE month_start = (SELECT max(month_start) - INTERVAL 1 MONTH FROM gold.kpi_monthly_actual_vs_budget)
          AND expense_variance_pct > 0
        ORDER BY expense_variance_pct DESC LIMIT 15""", "bar"),
    "👥 Revenue by customer segment (last 12 months)": ("""
        SELECT segment, round(sum(revenue_usd)) AS revenue, max(active_customers) AS peak_monthly_customers
        FROM gold.kpi_revenue_by_segment
        WHERE month_start > (SELECT max(month_start) - INTERVAL 12 MONTH FROM gold.kpi_revenue_by_segment)
          AND segment NOT IN ('Unknown', 'Not applicable')
        GROUP BY 1 ORDER BY revenue DESC""", "bar"),
    "🧾 Suppliers with the most overdue payables": ("""
        SELECT v.vendor_name, count(*) AS invoices, round(sum(f.open_amount_usd)) AS overdue_usd
        FROM gold.fact_ap_invoices f JOIN gold.dim_vendor v USING (vendor_id)
        WHERE f.days_past_due > 0 GROUP BY 1 ORDER BY overdue_usd DESC LIMIT 10""", "bar"),
    "🔮 Forecast revenue for the next 6 months": (None, "forecast"),
}
EXAMPLE_PROMPTS = [
    "Which department had the highest burn rate last month?",
    "Compare Enterprise vs SMB revenue by quarter",
    "Top 5 GL accounts by spend this year",
    "Forecast net profit for next quarter",
]


def _render_insight(name: str) -> None:
    sql, kind = INSIGHTS[name]
    if kind == "forecast":
        result = ai_agent.run_forecast_tool("forecast revenue for the next 6 months")
        _render_message({"role": "assistant", "figure": result.get("figure"), "summary": result.get("summary"),
                         "tool": "forecast"})
        return
    df = data.q(sql).copy()
    df.columns = [c.replace("_", " ").capitalize().replace("usd", "USD").replace("Dept code", "Dept") for c in df.columns]
    x, ys = df.columns[0], [c for c in df.select_dtypes("number").columns if c != df.columns[0]]
    if kind == "line":
        fig = px.line(df, x=x, y=ys[:3], markers=True)
        fig.update_layout(xaxis_title=None, yaxis_title=None)
        money_axis(fig)
    else:
        label = "Department name" if "Department name" in df else x
        value = ys[-1] if "pct" in ys[-1] else ys[0]
        fig = px.bar(df, x=value, y=label, orientation="h", text=df[value].map(
            (lambda v: f"{v:+.1f}%") if "pct" in value else T.money))
        fig.update_traces(marker_color=T.RED if "pct" in value else T.BLUE, textposition="outside",
                          textfont=dict(color=T.INK_2), cliponaxis=False)
        fig.update_layout(yaxis=dict(autorange="reversed", title=None), xaxis_title=None)
        if "pct" not in value:
            money_axis(fig, "x")
        pad_x(fig, df[value])
    with card():
        plot(fig, 360)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == object and col.lower().startswith("month"):
            df[col] = df[col].astype(str).str[:7]
    money_cols = {c: st.column_config.NumberColumn(c, format="compact") for c in ys if "pct" not in c and "invoices" not in c.lower()
                  and "customers" not in c.lower()}
    st.dataframe(df, hide_index=True, width="stretch", column_config=money_cols)
    with st.expander("🔍 SQL"):
        st.code(sql.strip(), language="sql")


def render_chat() -> None:
    hero("Ask the warehouse", 'AI <span class="grad">financial analyst</span>',
         "Natural language → guarded DuckDB SQL over the gold layer, with forecasting and automatic failover "
         "across free LLM providers.")
    providers_now = available_providers()

    if not providers_now:
        st.markdown(
            '<div class="callout">🔑 <b>The chat needs at least one free LLM API key.</b> On Streamlit Cloud open '
            '<b>Manage app → Settings → Secrets</b> and add any of '
            '<code>GROQ_API_KEY</code>, <code>GEMINI_API_KEY</code>, <code>MISTRAL_API_KEY</code>, '
            '<code>OPENROUTER_API_KEY</code> (TOML: <code>GROQ_API_KEY = "gsk_..."</code>), then reboot the app. '
            'Locally, put them in <code>.env</code>. Meanwhile the curated insights below run without any key.</div>',
            unsafe_allow_html=True,
        )
        st.write("")
        pick = st.pills("Instant insights (no LLM needed)", list(INSIGHTS), default=list(INSIGHTS)[0],
                        key="insight_pick")
        if pick:
            _render_insight(pick)
        return

    if "messages" not in st.session_state:
        st.session_state.messages = []
    with st.expander("⚡ Instant insights (no LLM call)", expanded=not st.session_state.messages):
        pick = st.pills("Insights", list(INSIGHTS), key="insight_pick", label_visibility="collapsed")
        if pick:
            _render_insight(pick)

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            _render_message(msg)

    chosen = None
    if not st.session_state.messages:
        chosen = st.pills("Try asking", EXAMPLE_PROMPTS, key="example_prompt")
    prompt = st.chat_input("e.g. Which department had the highest burn rate last month?") or chosen
    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"), st.spinner("Analyzing..."):
            result = ai_agent.handle_prompt(prompt, provider_key=provider_key)
            assistant_msg: dict = {"role": "assistant"}
            if result.get("error"):
                assistant_msg["content"] = f"⚠️ {result['error']}"
            elif result.get("tool") == "forecast":
                assistant_msg["figure"] = result.get("figure")
                assistant_msg["summary"] = result.get("summary")
                assistant_msg["narrative"] = result.get("narrative")
                assistant_msg["tool"] = "forecast"
                assistant_msg["engine"] = result.get("summary", {}).get("engine")
            else:
                assistant_msg["content"] = result.get("narrative") or "_Query executed._"
                assistant_msg["dataframe"] = result.get("dataframe")
                assistant_msg["sql"] = result.get("sql")
                assistant_msg["figure"] = result.get("figure")
                assistant_msg["provider"] = result.get("provider_label") or result.get("provider")
                assistant_msg["model"] = result.get("model")
                assistant_msg["latency_ms"] = result.get("latency_ms")
                assistant_msg["attempts"] = result.get("attempts")
                assistant_msg["tool"] = result.get("tool")
                if result.get("fallback_reason"):
                    assistant_msg["note"] = result["fallback_reason"]
            st.session_state.messages.append(assistant_msg)
            _render_message(assistant_msg)
        if chosen:
            st.rerun()


def _render_message(msg: dict) -> None:
    if msg["role"] == "user":
        st.markdown(msg["content"])
        return
    if msg.get("content"):
        st.markdown(msg["content"])
    if msg.get("narrative"):
        st.markdown(msg["narrative"])
    if msg.get("note"):
        st.info(msg["note"])
    if msg.get("summary"):
        s = msg["summary"]
        m1, m2, m3 = st.columns(3)
        m1.metric(f"Next-month {s['metric'].lower()}", T.money(s["next_month_forecast"]), border=True)
        m2.metric(f"{s['horizon_months']}-month projection", T.money(s["period_total"]), border=True)
        m3.metric("Trend vs last actual", f"{s['trend_vs_last_actual_pct']:+.1f}%", border=True)
        st.caption(f"Forecast engine: `{s['engine']}`")
    if msg.get("figure") is not None:
        st.plotly_chart(msg["figure"], width="stretch", config=PLOT_CONFIG)
    if isinstance(msg.get("dataframe"), pd.DataFrame):
        st.dataframe(msg["dataframe"], width="stretch", hide_index=True)
    if msg.get("sql"):
        with st.expander("🔍 Generated SQL"):
            st.code(msg["sql"], language="sql")
    if msg.get("attempts"):
        with st.expander("🔀 Router trace"):
            st.json(msg["attempts"])
    badges = [f"`{msg[k]}`" for k in ("provider", "model") if msg.get(k)]
    if msg.get("latency_ms"):
        badges.append(f"`{msg['latency_ms']} ms`")
    if msg.get("tool"):
        badges.append(f"tool: `{msg['tool']}`")
    if badges:
        st.caption(" · ".join(badges))


# --------------------------------------------------------------------------- data quality
ACTION_COLORS = {"quarantined": T.RED, "repaired": T.BLUE, "merged": T.VIOLET, "flagged": T.YELLOW,
                 "deduplicated": T.AQUA, "applied": T.ORANGE, "removed": T.MAGENTA, "filled": T.GREEN,
                 "versioned": T.MUTED}


def render_data_quality() -> None:
    hero("Trust layer", 'Data <span class="grad">quality</span>',
         "Every silver rule and gold production test writes to <code>audit.dq_results</code>; rejected records "
         "stay in <code>audit.quarantine</code> with the reason so they can be fixed at source and replayed.")
    if not data.table_exists("audit", "dq_results"):
        st.info("No pipeline run recorded in this warehouse yet.")
        return
    runs = data.q("SELECT DISTINCT run_id FROM audit.dq_results ORDER BY run_id DESC")
    run_id = st.selectbox("Pipeline run", runs["run_id"].tolist())
    results = data.q("SELECT * FROM audit.dq_results WHERE run_id = ?", (run_id,))
    silver = results[results["layer"] == "silver"]
    gold = results[results["layer"] == "gold"]

    failed = gold[(gold["failed_rows"] > 0) & (gold["severity"] == "error")]
    landed = int(silver.loc[silver["check_name"] == "rows_landed", "total_rows"].sum())
    quarantined = int(silver.loc[silver["action"] == "quarantined", "failed_rows"].sum())
    c = st.columns(4)
    c[0].metric("Rows landed", T.count(landed), border=True)
    c[1].metric("Rows quarantined", T.count(quarantined),
                f"{100 * quarantined / landed:.2f}% of landed" if landed else None, delta_color="off", border=True)
    c[2].metric("Repaired · merged",
                T.count(silver.loc[silver["action"].isin(["repaired", "flagged", "merged"]), "failed_rows"].sum()),
                border=True)
    c[3].metric("Gold tests passed", f"{len(gold) - len(failed)} / {len(gold)}",
                "all green" if failed.empty else f"{len(failed)} failing", delta_color="normal" if failed.empty else "inverse",
                border=True)

    left, right = st.columns([3, 2])
    with left, card():
        view = silver[(silver["check_name"] != "rows_landed") & (silver["failed_rows"] > 0)].sort_values("failed_rows")
        fig = px.bar(view, x="failed_rows", y="check_name", color="action", orientation="h", log_x=True,
                     color_discrete_map=ACTION_COLORS, hover_data=["entity", "pass_pct"],
                     title="Silver rules — rows affected (log scale)")
        fig.update_layout(yaxis_title=None, xaxis_title=None, height=max(420, 22 * len(view)), legend_title_text="",
                          legend=dict(y=-0.02, yanchor="top", x=0, xanchor="left"), margin=dict(t=48, b=60))
        st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG)
    with right:
        with card():
            ent = silver[silver["action"] == "quarantined"].groupby("entity", as_index=False)["failed_rows"].sum()
            fig = px.pie(ent, names="entity", values="failed_rows", hole=0.62, title="Quarantine by entity")
            fig.update_traces(textinfo="percent", textposition="inside", textfont=dict(color="#ffffff"),
                              marker=dict(colors=T.SERIES[:len(ent)]))
            plot(fig, 260)
        with card():
            st.markdown("**Gold production tests**")
            g = gold[["entity", "check_name", "severity", "failed_rows"]].rename(columns={"entity": "table"})
            g.insert(0, "status", g["failed_rows"].map(lambda n: "✅ pass" if n == 0 else "❌ fail"))
            st.dataframe(g, width="stretch", hide_index=True, height=300)

    st.markdown("#### Quarantined records")
    reasons = data.q("SELECT entity, reason, count(*) AS rows FROM audit.quarantine WHERE run_id = ? "
                     "GROUP BY 1, 2 ORDER BY 3 DESC", (run_id,))
    if reasons.empty:
        st.caption("Nothing quarantined in this run.")
        return
    pick = st.selectbox("Reason", [f"{r.entity} · {r.reason} ({r.rows:,})" for r in reasons.itertuples()])
    entity, reason = pick.split(" (")[0].split(" · ")
    sample = data.q("SELECT source_file, record FROM audit.quarantine WHERE run_id = ? AND entity = ? AND reason = ? "
                    "LIMIT 25", (run_id, entity, reason))
    sample["source_file"] = sample["source_file"].str.replace(chr(92), "/", regex=False).str.split("/bronze/").str[-1]
    st.dataframe(sample, width="stretch", hide_index=True)


# --------------------------------------------------------------------------- explorer
def render_explorer() -> None:
    hero("Self-service", 'Data <span class="grad">explorer</span>', "Browse any gold, silver or audit table and run read-only SQL.")
    tables = data.q("SELECT schema_name || '.' || table_name AS t, estimated_size AS n FROM duckdb_tables() "
                    "WHERE schema_name IN ('gold', 'silver', 'audit') ORDER BY 1")
    sizes = dict(zip(tables["t"], tables["n"]))
    left, right = st.columns([2, 1])
    options = tables["t"].tolist()
    chosen = left.selectbox("Table", options, index=options.index("gold.fact_transactions") if "gold.fact_transactions" in options else 0,
                            format_func=lambda t: f"{t}  ·  {sizes[t]:,} rows")
    limit = right.select_slider("Preview rows", [50, 200, 1000], value=200)
    preview = data.q(f"SELECT * FROM {chosen} LIMIT {int(limit)}")
    st.dataframe(preview, width="stretch", hide_index=True, height=380)
    st.download_button("⬇️ Download preview as CSV", preview.to_csv(index=False).encode(),
                       file_name=f"{chosen.replace('.', '_')}.csv")

    st.markdown("#### Run your own read-only SQL")
    default_sql = (
        "SELECT dept_code,\n       SUM(revenue_actual) AS revenue,\n       SUM(expense_actual) AS expense\n"
        "FROM gold.kpi_monthly_actual_vs_budget\nGROUP BY dept_code ORDER BY revenue DESC"
    )
    sql = st.text_area("SQL (SELECT/WITH only)", value=default_sql, height=140)
    if st.button("▶️ Execute", type="primary"):
        from src.ai.text_to_sql import enforce_limit, sanitize_sql

        try:
            safe = enforce_limit(sanitize_sql(sql), max_rows=500)
            st.dataframe(data.query(safe), width="stretch", hide_index=True)
        except ValueError as exc:
            st.error(f"Blocked: {exc}")
        except duckdb.Error as exc:
            st.error(f"Query failed: {exc}")


# --------------------------------------------------------------------------- pipeline ops
def render_pipeline_ops() -> None:
    hero("Operations", 'Pipeline <span class="grad">control room</span>', "Trigger individual pipeline steps against this instance.")
    broker = EventBroker()
    stats = bronze_api.bronze_stats()
    c = st.columns(5)
    c[0].metric("Bronze transactions", T.count(stats["transactions"]), border=True)
    c[1].metric("Bronze AP invoices", T.count(stats["ap_invoices"]), border=True)
    c[2].metric("Bronze budgets", T.count(stats["budgets"]), border=True)
    c[3].metric("Bronze masters", T.count(stats["masters"]), border=True)
    c[4].metric("Bronze FX rates", T.count(stats["fx_rates"]), border=True)
    if stats["transactions"] == 0:
        st.info("This instance serves the nightly warehouse snapshot, so its bronze lake is empty. "
                "Generate sources (step 0) before re-running the notebooks here.")

    col_a, col_b = st.columns(2)
    with col_a, card():
        rows = st.number_input("Fact rows to generate", min_value=50_000, max_value=settings.SIM_TARGET_FACT_ROWS,
                               value=min(500_000, settings.SIM_TARGET_FACT_ROWS), step=50_000)
        if st.button("🏭 0. Generate source history → bronze", width="stretch"):
            with st.status("Generating ERP, POS, Kafka, AP, FX, budgets and master data...", expanded=True) as status:
                summary = ingest.run_full_ingest(include_ocr=False, target_rows=int(rows), reset=True)
                st.json(summary, expanded=False)
                status.update(label=f"Done in {summary['seconds']}s", state="complete")
        days = st.number_input("Simulate N days of stream events", min_value=1, max_value=60, value=7)
        if st.button("📡 1. Publish stream events", width="stretch"):
            with st.status("Producing events...", expanded=True) as status:
                sent = ingest.publish_stream(days=int(days), txns_per_day=settings.SIM_TXNS_PER_DAY)
                st.write(f"Published **{sent}** events → {broker.description}")
                status.update(label="Done", state="complete")
        if st.button("📥 2. Drain stream → bronze", width="stretch"):
            with st.status("Consuming...", expanded=True) as status:
                st.write(ingest.drain_stream_to_bronze())
                status.update(label="Done", state="complete")
        if st.button("🗂 3. Land files / SharePoint / OCR invoices", width="stretch"):
            with st.status("Landing auxiliary sources...", expanded=True) as status:
                uploads = ingest.land_file_uploads()
                sp = ingest.land_sharepoint()
                inv = ingest.land_invoices(render_first=True)
                st.write({"file_uploads": len(uploads), "sharepoint": sp, "ocr_invoices": len(inv)})
                status.update(label="Done", state="complete")
    with col_b, card():
        if st.button("📓 4. Run notebooks (bronze → silver → gold)", width="stretch", type="primary",
                     disabled=stats["transactions"] == 0):
            with st.status("Running 01_bronze_to_silver and 02_silver_to_gold into staging...", expanded=True) as status:
                data.release()  # free the file so the tested build can replace it
                try:
                    meta = transform.run_transforms()
                except RuntimeError as exc:
                    st.error(f"Build rejected, live warehouse unchanged: {exc}")
                    status.update(label="Failed", state="error")
                else:
                    st.json(meta["gold_row_counts"])
                    status.update(label="Published", state="complete")
        if st.button("☁️ 5. Sync external warehouses", width="stretch"):
            with st.status("Syncing...", expanded=True) as status:
                results = sync_all_warehouses()
                st.json({k: v.get("status") for k, v in results.items()})
                status.update(label="Done", state="complete")
        if data.table_exists("audit", "pipeline_runs"):
            st.markdown("**Recent notebook runs**")
            st.dataframe(data.query("SELECT run_id, notebook, status, seconds, finished_at FROM audit.pipeline_runs "
                                    "ORDER BY finished_at DESC LIMIT 8"), width="stretch", hide_index=True)

    left, right = st.columns(2)
    with left, st.expander("Last transform metadata"):
        st.json(load_meta("transform_meta.json") or {"status": "no transform run yet"}, expanded=False)
    with right, st.expander("Last warehouse sync"):
        st.json(load_meta("sync_meta.json") or {"status": "no sync run yet"}, expanded=False)


PAGES = {
    "🏠 Overview": render_overview,
    "📈 Dashboard": render_dashboard,
    "🤖 AI Analyst": render_chat,
    "🧪 Data Quality": render_data_quality,
    "🔍 Data Explorer": render_explorer,
    "🔄 Pipeline Ops": render_pipeline_ops,
}

PAGES[page]()
_warm_caches(VERSION)  # after the first render, so it never competes with the page the visitor asked for
