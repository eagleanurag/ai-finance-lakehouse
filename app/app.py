from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
from src.pipelines import ingest, transform
from src.pipelines.sync import sync_all_warehouses
from src.sources.kafka_client import EventBroker


def _q(sql: str) -> pd.DataFrame:
    import duckdb

    con = duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


@st.cache_resource(show_spinner="Bootstrapping lakehouse: simulating sources -> Kafka -> bronze -> silver/gold...")
def ensure_warehouse() -> bool:
    if settings.DUCKDB_PATH.exists():
        try:
            df = _q("SELECT COUNT(*) AS n FROM gold.fact_transactions")
            if df["n"].iloc[0] > 0:
                return True
        except Exception:
            pass
    ingest.run_full_ingest(stream_days=400, budget_months=18, include_ocr=False)
    transform.run_transforms()
    return True


def load_meta(name: str) -> dict | None:
    path = settings.DATA_DIR / name
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


ensure_warehouse()

with st.sidebar:
    st.markdown("## 📊 AI Finance Lakehouse")
    st.caption("Multi-source · Multi-warehouse · GenAI analytics — $0 cloud cost")

    page = st.radio(
        "Navigate",
        ["🏠 Overview", "📈 Dashboard", "🤖 AI Analyst", "🔍 Data Explorer", "🔄 Pipeline Ops"],
        label_visibility="collapsed",
    )

    st.divider()
    providers = available_providers()
    provider_options = ["Auto (Smart Fallback)"] + [p["label"] for p in providers]
    provider_map = {p["label"]: p["key"] for p in providers}
    selected_label = st.selectbox("AI Model", provider_options, index=0)
    provider_key = "auto" if selected_label.startswith("Auto") else provider_map[selected_label]

    st.divider()
    broker = EventBroker()
    st.markdown(f"**Streaming:** {broker.description}")
    duck_path = settings.DUCKDB_PATH
    st.markdown(f"**Warehouse:** `{duck_path.name}`")

    if st.button("♻️ Clear cache & refresh"):
        st.cache_resource.clear()
        st.rerun()

    meta = load_meta("transform_meta.json")
    if meta:
        ts = str(meta.get("last_transform_utc", ""))[:16].replace("T", " ")
        st.caption(f"Last transform: {ts} UTC")


def render_overview() -> None:
    st.title("Enterprise Multi-Source Lakehouse + GenAI Analytics")
    st.markdown(
        "A production-style **data engineering platform** that ingests streaming, file, SharePoint and OCR "
        "sources into a medallion architecture, serves a live BI dashboard, and answers financial questions "
        "through an LLM analyst with automatic multi-provider failover — built entirely on free tiers."
    )

    col1, col2, col3, col4 = st.columns(4)
    stats = bronze_api.bronze_stats()
    txn_count = _q("SELECT COUNT(*) n FROM gold.fact_transactions").iloc[0]["n"]
    months = _q("SELECT COUNT(DISTINCT year_month) n FROM gold.dim_date").iloc[0]["n"]
    col1.metric("Fact rows", f"{txn_count:,}")
    col2.metric("Bronze events", f"{stats['transactions']:,}")
    col3.metric("Months covered", months)
    col4.metric(
        "Source channels",
        _q("SELECT COUNT(DISTINCT source_channel) n FROM gold.fact_transactions").iloc[0]["n"],
    )

    st.markdown("### Architecture")
    st.code(
        """┌─────────────────────────────── SOURCES ────────────────────────────────┐
│  📡 Streaming txns      📁 CSV/Excel uploads     🏢 SharePoint/Graph    │
│  (Kafka / Upstash       (Finance team            (Master data sync)     │
│   REST or file broker)   budget exports)          🧾 Invoice images+OCR  │
└──────────┬────────────────────────┬──────────────────────┬─────────────┘
           ▼                        ▼                      ▼
┌────────────────────────────── BRONZE LAKE ─────────────────────────────┐
│      Raw JSONL partitions → local lake dir / Azure Blob (ADLS Gen2)    │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   ▼
┌──────────────────── SILVER (DuckDB SQL transforms) ────────────────────┐
│         typed · deduplicated · standardized entity tables              │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   ▼
┌────────────────────── GOLD STAR SCHEMA + KPIs ─────────────────────────┐
│  dim_date · dim_department · dim_gl_account · dim_vendor               │
│  fact_transactions · fact_budget                                       │
│  kpi_monthly_actual_vs_budget · monthly_financials                     │
└──────────┬──────────────────────────────┬──────────────────────────────┘
           ▼                              ▼
┌────────── BI SERVING ─────────┐  ┌──────── AI ANALYST ────────────────┐
│  Power BI publish-to-web      │  │  LLM router w/ auto-failover:      │
│  + native Plotly KPI app      │  │  Groq → Gemini → Mistral → GitHub  │
│                               │  │  Tools: Text-to-SQL · Forecasting  │
└───────────────────────────────┘  └────────────────────────────────────┘""",
        language=None,
    )

    left, right = st.columns(2)
    with left:
        st.markdown("### Stack & free tiers")
        st.markdown(
            "| Layer | Tech | Free tier |\n"
            "|---|---|---|\n"
            "| Streaming | Upstash Kafka REST | Forever free (10k msg/day) |\n"
            "| Bronze lake | Local dir → Azure Blob | 5 GB always-free blob |\n"
            "| Warehouse | DuckDB → MotherDuck | 10 GB free developer plan |\n"
            "| OLAP warehouse | BigQuery Sandbox | 10 GB + 1 TB query/mo forever |\n"
            "| OLTP serving | Neon Postgres | 0.5 GB serverless forever |\n"
            "| Orchestration | GitHub Actions cron | 2,000 min/mo free |\n"
            "| BI | Power BI publish-to-web | Free desktop + public embed |\n"
            "| App host | Streamlit Community Cloud | Forever free |\n"
            "| LLMs | Groq / Gemini / Mistral / GitHub Models | Free dev tiers |"
        )
    with right:
        st.markdown("### Source connector mix")
        channel_df = _q(
            """
            SELECT source_channel AS channel, COUNT(*) AS events,
                   ROUND(SUM(amount_usd), 0) AS total_usd
            FROM gold.fact_transactions
            GROUP BY source_channel ORDER BY events DESC
            """
        )
        st.dataframe(channel_df, width="stretch", hide_index=True)
        st.markdown("### Gold tables")
        tables = _q(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='gold' ORDER BY table_name"
        )
        st.dataframe(tables, width="stretch", hide_index=True)


def render_dashboard() -> None:
    st.title("📈 Executive Finance Dashboard")

    kpis = _q(
        """
        SELECT
            SUM(revenue_actual) AS revenue,
            SUM(expense_actual) AS expense,
            ROUND(AVG(net_margin_pct), 1) AS avg_margin,
            ROUND(AVG(burn_rate), 2) AS avg_burn
        FROM gold.kpi_monthly_actual_vs_budget
        WHERE month_start >= (SELECT MAX(month_start) - INTERVAL 11 MONTH FROM gold.kpi_monthly_actual_vs_budget)
        """
    ).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Revenue (12 mo)", f"${kpis['revenue']:,.0f}")
    c2.metric("Expenses (12 mo)", f"${kpis['expense']:,.0f}")
    c3.metric("Avg Net Margin", f"{kpis['avg_margin']}%")
    c4.metric("Avg Burn Rate", f"{kpis['avg_burn']}x")

    monthly = _q("SELECT * FROM gold.monthly_financials ORDER BY month_start")
    monthly["month_start"] = monthly["month_start"].astype(str)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=monthly["month_start"], y=monthly["total_revenue"], name="Revenue", fill="tozeroy"))
    fig.add_trace(go.Scatter(x=monthly["month_start"], y=monthly["total_expense"], name="Expense", fill="tozeroy"))
    fig.add_trace(go.Scatter(x=monthly["month_start"], y=monthly["net_profit"], name="Net Profit", mode="lines"))
    fig.update_layout(title="Monthly revenue vs expense vs net profit", height=380, legend=dict(orientation="h"))

    kpi = _q(
        """
        SELECT month_start, dept_code, expense_actual, budget_expense,
               expense_variance_pct, burn_rate
        FROM gold.kpi_monthly_actual_vs_budget
        WHERE month_start = (SELECT MAX(month_start) FROM gold.kpi_monthly_actual_vs_budget)
        ORDER BY expense_actual DESC
        """
    )
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=kpi["dept_code"], y=kpi["expense_actual"], name="Actual expense"))
    fig2.add_trace(
        go.Scatter(x=kpi["dept_code"], y=kpi["budget_expense"], name="Budget", mode="markers", marker=dict(size=12))
    )
    fig2.update_layout(
        title=f"Departmental spend vs budget — {kpi['month_start'].iloc[0]}",
        height=380,
        legend=dict(orientation="h"),
    )

    left, right = st.columns([3, 2])
    with left:
        st.plotly_chart(fig, width="stretch")
    with right:
        cat = _q(
            """
            SELECT a.category AS category, SUM(f.amount_usd) AS amount
            FROM gold.fact_transactions f
            JOIN gold.dim_gl_account a USING (account_id)
            WHERE a.category <> 'Revenue'
            GROUP BY a.category
            """
        )
        pie = px.pie(cat, names="category", values="amount", hole=0.45, title="Spend mix by GL category")
        pie.update_layout(height=380, margin=dict(t=50))
        st.plotly_chart(pie, width="stretch")

    st.plotly_chart(fig2, width="stretch")

    st.markdown("### Department scorecard (latest month)")
    st.dataframe(kpi, width="stretch", hide_index=True)

    st.divider()
    st.markdown("### Power BI dashboard")
    embed_url = settings.POWERBI_EMBED_URL
    user_url = None
    with st.expander("⚙️ Dashboard source", expanded=not bool(embed_url)):
        user_url = st.text_input(
            "Publish-to-web URL",
            value=embed_url,
            placeholder="https://app.powerbi.com/view?r=...",
            help="In Power BI Service: File → Embed report → Publish to web (public). Paste the link here or set POWERBI_EMBED_URL.",
        )
    final_url = (user_url or "").strip()
    if final_url.startswith("http"):
        components.iframe(final_url, height=620, scrolling=True)
    else:
        st.info(
            "**No Power BI report connected yet.** Publish your report to the web and paste the URL above — "
            "it will render here publicly for recruiters, no login required. Meanwhile, all KPIs above are "
            "rendered natively from the gold layer."
        )


def render_chat() -> None:
    st.title("🤖 AI Financial Analyst")
    providers_now = available_providers()
    if not providers_now:
        st.warning(
            "No LLM API keys configured. Add `GROQ_API_KEY` / `GEMINI_API_KEY` / `MISTRAL_API_KEY` / "
            "`OPENROUTER_API_KEY` to `.env` locally or to this Space's/app's secrets. "
            "All non-AI features keep working."
        )
        st.stop()

    st.caption(
        "Ask about revenue, expenses, budgets, margins, departments — or request forecasts "
        "(e.g. *\"forecast revenue for next quarter\"*). Auto mode fails over across free providers on rate limits."
    )

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            _render_message(msg)

    prompt = st.chat_input("e.g. Which department had the highest burn rate last month?")
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
        m1.metric(f"Next-month {s['metric'].lower()}", f"${s['next_month_forecast']:,.0f}")
        m2.metric(f"Total {s['horizon_months']}-mo projection", f"${s['period_total']:,.0f}")
        m3.metric("Trend vs last actual", f"{s['trend_vs_last_actual_pct']:+.1f}%")
        st.caption(f"Forecast engine: `{s['engine']}`")
    if msg.get("figure") is not None:
        st.plotly_chart(msg["figure"], width="stretch")
    if isinstance(msg.get("dataframe"), pd.DataFrame):
        st.dataframe(msg["dataframe"], width="stretch", hide_index=True)
    if msg.get("sql"):
        with st.expander("🔍 Generated SQL"):
            st.code(msg["sql"], language="sql")
    if msg.get("attempts"):
        with st.expander("🔀 Router trace"):
            st.json(msg["attempts"])
    badges = []
    if msg.get("provider"):
        badges.append(f"`{msg['provider']}`")
    if msg.get("model"):
        badges.append(f"`{msg['model']}`")
    if msg.get("latency_ms"):
        badges.append(f"`{msg['latency_ms']} ms`")
    if msg.get("tool"):
        badges.append(f"tool: `{msg['tool']}`")
    if badges:
        st.caption(" · ".join(badges))


def render_explorer() -> None:
    st.title("🔍 Data Explorer")
    tables = [
        r[0]
        for r in _q(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables "
            "WHERE table_schema IN ('silver','gold') ORDER BY 1"
        ).values.tolist()
    ]
    chosen = st.selectbox("Table", tables)
    preview = _q(f"SELECT * FROM {chosen} LIMIT 200")
    st.dataframe(preview, width="stretch", hide_index=True)
    st.download_button(
        "⬇️ Download CSV",
        preview.to_csv(index=False).encode(),
        file_name=f"{chosen.replace('.', '_')}.csv",
    )

    st.divider()
    st.markdown("### Run your own read-only SQL")
    default_sql = (
        "SELECT dept_code,\n"
        "       SUM(revenue_actual) AS revenue,\n"
        "       SUM(expense_actual) AS expense\n"
        "FROM gold.kpi_monthly_actual_vs_budget\n"
        "GROUP BY dept_code ORDER BY revenue DESC"
    )
    sql = st.text_area("SQL (SELECT/WITH only)", value=default_sql, height=140)
    if st.button("▶️ Execute"):
        from src.ai.text_to_sql import enforce_limit, sanitize_sql

        try:
            safe = enforce_limit(sanitize_sql(sql), max_rows=500)
            st.dataframe(_q(safe), width="stretch", hide_index=True)
        except ValueError as exc:
            st.error(f"Blocked: {exc}")


def render_pipeline_ops() -> None:
    st.title("🔄 Pipeline Operations")
    st.caption("Trigger individual pipeline steps against the live lakehouse.")

    broker = EventBroker()
    stats = bronze_api.bronze_stats()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Bronze txns", f"{stats['transactions']:,}")
    c2.metric("Bronze invoices", stats["invoices"])
    c3.metric("Bronze budgets", f"{stats['budgets']:,}")
    c4.metric("Bronze masters", stats["masters"])

    col_a, col_b = st.columns(2)
    with col_a:
        days = st.number_input("Simulate N days of transactions", min_value=1, max_value=60, value=7)
        if st.button("📡 1. Publish stream events", width="stretch"):
            with st.status("Producing events...", expanded=True) as status:
                sent = ingest.publish_stream(days=int(days), txns_per_day=settings.SIM_TXNS_PER_DAY)
                st.write(f"Published **{sent}** events → {broker.description}")
                status.update(label="Done", state="complete")
        if st.button("📥 2. Drain stream → bronze", width="stretch"):
            with st.status("Consuming...", expanded=True) as status:
                res = ingest.drain_stream_to_bronze()
                st.write(json.dumps(res, indent=2))
                status.update(label="Done", state="complete")
        if st.button("🗂 3. Land files / SharePoint / OCR invoices", width="stretch"):
            with st.status("Landing auxiliary sources...", expanded=True) as status:
                budgets = ingest.land_file_uploads()
                sp = ingest.land_sharepoint()
                inv = ingest.land_invoices(render_first=True)
                st.write({"file_uploads": budgets, "sharepoint": sp, "ocr_invoices": len(inv)})
                status.update(label="Done", state="complete")
    with col_b:
        if st.button("🧱 4. Run transforms (bronze→silver→gold)", width="stretch"):
            with st.status("Transforming...", expanded=True) as status:
                meta = transform.run_transforms()
                st.write(json.dumps(meta["gold_row_counts"], indent=2))
                status.update(label="Done", state="complete")
        if st.button("☁️ 5. Sync external warehouses", width="stretch"):
            with st.status("Syncing...", expanded=True) as status:
                results = sync_all_warehouses()
                st.json({k: v.get("status") for k, v in results.items()})
                status.update(label="Done", state="complete")

    st.divider()
    tmeta = load_meta("transform_meta.json")
    smeta = load_meta("sync_meta.json")
    left, right = st.columns(2)
    with left:
        st.markdown("**Last transform**")
        if tmeta:
            st.json(tmeta, expanded=False)
        else:
            st.caption("No transform run yet.")
    with right:
        st.markdown("**Last warehouse sync**")
        if smeta:
            st.json(smeta, expanded=False)
        else:
            st.caption("No sync run yet.")


PAGES = {
    "🏠 Overview": render_overview,
    "📈 Dashboard": render_dashboard,
    "🤖 AI Analyst": render_chat,
    "🔍 Data Explorer": render_explorer,
    "🔄 Pipeline Ops": render_pipeline_ops,
}

PAGES[page]()
