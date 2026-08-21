from __future__ import annotations

import re
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from config import settings
from src.ai import forecaster, text_to_sql
from src.pipelines.transform import get_connection

FORECAST_KEYWORDS = [
    "forecast", "predict", "projection", "project", "next month", "next quarter",
    "next year", "upcoming", "future", "plan for", "planning", "expect", "outlook",
    "estimate next", "will be",
]

MONTHLY_METRIC_COLUMNS = {
    "revenue": "total_revenue",
    "total_expense": "total_expense",
    "net_profit": "net_profit",
}


def is_forecast_intent(prompt: str) -> bool:
    lowered = prompt.lower()
    return any(kw in lowered for kw in FORECAST_KEYWORDS)


def _load_monthly() -> pd.DataFrame:
    con = get_connection()
    try:
        return con.execute(
            "SELECT * FROM gold.monthly_financials ORDER BY month_start"
        ).fetchdf()
    finally:
        con.close()


def run_forecast_tool(prompt: str) -> dict[str, Any]:
    metric = forecaster.detect_metric(prompt) or "revenue"
    horizon = forecaster.detect_horizon(prompt)
    value_col = MONTHLY_METRIC_COLUMNS[metric]
    monthly = _load_monthly()
    result = forecaster.forecast_monthly(monthly, value_col=value_col, horizon_months=horizon)
    if "error" in result:
        return {"tool": "forecast", **result}

    fc = result["forecast"]
    hist = monthly[["month_start", value_col]].rename(columns={value_col: "value"})

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=hist["month_start"], y=hist["value"], name="Actual", mode="lines+markers"))
    fig.add_trace(
        go.Scatter(x=fc["ds"], y=fc["yhat"], name="Forecast", mode="lines+markers", line=dict(dash="dash"))
    )
    fig.add_trace(
        go.Scatter(
            x=list(fc["ds"]) + list(fc["ds"])[::-1],
            y=list(fc["yhat_upper"]) + list(fc["yhat_lower"])[::-1],
            fill="toself",
            fillcolor="rgba(99,110,250,0.15)",
            line=dict(width=0),
            hoverinfo="skip",
            name="Confidence band",
        )
    )
    metric_label = metric.replace("_", " ").title()
    fig.update_layout(
        title=f"{metric_label} - Actual vs Forecast (next {horizon} months)",
        yaxis_title="USD",
        legend=dict(orientation="h"),
        margin=dict(t=50, b=30),
    )

    total_projected = float(fc["yhat"].sum())
    delta_pct = (
        (fc["yhat"].iloc[-1] - hist["value"].iloc[-1]) / abs(hist["value"].iloc[-1]) * 100
        if len(hist)
        else 0
    )
    summary = {
        "metric": metric_label,
        "engine": result["engine"],
        "horizon_months": horizon,
        "next_month_forecast": float(fc["yhat"].iloc[0]),
        "end_of_period_forecast": float(fc["yhat"].iloc[-1]),
        "period_total": total_projected,
        "trend_vs_last_actual_pct": float(delta_pct),
        "table": fc.to_dict(orient="records"),
    }
    return {"tool": "forecast", "figure": fig, "summary": summary}


def run_sql_tool(prompt: str, provider_key: str) -> dict[str, Any]:
    sql_result = text_to_sql.execute_text_to_sql(prompt, provider_key=provider_key)
    if "error" in sql_result:
        return {"tool": "text_to_sql", **sql_result}

    df: pd.DataFrame = sql_result["dataframe"]
    figure = None
    if len(df.columns) >= 2 and len(df) > 1:
        date_like = next(
            (c for c in df.columns if any(k in c.lower() for k in ("month", "date", "week", "quarter"))), None
        )
        numeric_cols = [c for c in df.select_dtypes("number").columns if c not in ("month_sk", "date_sk")]
        if date_like and numeric_cols:
            plot_df = df.copy()
            plot_df[date_like] = plot_df[date_like].astype(str)
            if len(numeric_cols) == 1:
                figure = px.line(plot_df, x=date_like, y=numeric_cols[0], markers=True)
            elif len(df) <= 24:
                figure = px.bar(plot_df, x=date_like, y=numeric_cols[:4], barmode="group")
            else:
                figure = px.line(plot_df, x=date_like, y=numeric_cols[:3])

    narrative = _narrate(prompt, df, provider_key)
    return {
        "tool": "text_to_sql",
        "dataframe": df,
        "sql": sql_result.get("sql"),
        "figure": figure,
        "provider": sql_result.get("provider"),
        "provider_label": sql_result.get("provider_label"),
        "model": sql_result.get("model"),
        "latency_ms": sql_result.get("latency_ms"),
        "attempts": sql_result.get("llm_attempts"),
        "narrative": narrative,
    }


def _narrate(question: str, df: pd.DataFrame, provider_key: str) -> str | None:
    from src.ai.router import available_providers, chat

    chain = [provider_key] if provider_key != "auto" else [p["key"] for p in available_providers()]
    if not chain:
        return None
    compact = df.head(12).to_markdown(index=False)
    messages = [
        {
            "role": "system",
            "content": "You are a concise CFO-level analyst. In 3-5 sentences, answer the user's question using ONLY the query results provided. Quote concrete numbers. Never invent data.",
        },
        {"role": "user", "content": f"Question: {question}\n\nQuery results:\n{compact}"},
    ]
    result = chat(messages, provider_key=provider_key, max_tokens=400)
    return result.get("content")


def handle_prompt(prompt: str, provider_key: str = "auto") -> dict[str, Any]:
    prompt = prompt.strip()
    if not prompt:
        return {"error": "Empty prompt"}
    if is_forecast_intent(prompt):
        result = run_forecast_tool(prompt)
        if "error" not in result:
            narrative = _narrate_forecast(prompt, result["summary"], provider_key)
            result["narrative"] = narrative
            return result
        fallback = run_sql_tool(prompt, provider_key)
        fallback["fallback_reason"] = f"Forecast tool unavailable ({result['error']}); used Text-to-SQL instead"
        return fallback
    return run_sql_tool(prompt, provider_key)


def _narrate_forecast(question: str, summary: dict[str, Any], provider_key: str) -> str | None:
    from src.ai.router import available_providers, chat

    chain = [provider_key] if provider_key != "auto" else [p["key"] for p in available_providers()]
    if not chain:
        return None
    messages = [
        {
            "role": "system",
            "content": (
                "You are a financial planning analyst. Summarize this forecast in 3-5 sentences "
                "for an executive audience: state the projected numbers, the trend direction, and one practical implication. Use only the JSON provided."
            ),
        },
        {"role": "user", "content": f"Question: {question}\n\nForecast summary JSON:\n{summary}"},
    ]
    result = chat(messages, provider_key=provider_key, max_tokens=400)
    return result.get("content")
