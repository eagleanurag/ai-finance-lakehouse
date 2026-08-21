from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

FORECAST_ENGINE: str | None = None
try:
    from prophet import Prophet

    FORECAST_ENGINE = "prophet"
except ImportError:
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing

        FORECAST_ENGINE = "statsmodels_holt_winters"
    except ImportError:
        FORECAST_ENGINE = None


def forecast_monthly(
    monthly_df: pd.DataFrame,
    value_col: str,
    horizon_months: int = 6,
    confidence: float = 0.9,
) -> dict[str, Any]:
    series = (
        monthly_df[["month_start", value_col]]
        .dropna()
        .sort_values("month_start")
        .rename(columns={"month_start": "ds", value_col: "y"})
        .reset_index(drop=True)
    )
    if len(series) < 6:
        return {"error": f"Need at least 6 months of history for forecasting; found {len(series)}."}

    z = 1.96 if confidence >= 0.95 else 1.645

    if FORECAST_ENGINE == "prophet":
        model = Prophet(interval_width=confidence, yearly_seasonality=True)
        model.fit(series[["ds", "y"]])
        future = model.make_future_dataframe(periods=horizon_months, freq="MS")
        forecast = model.predict(future).tail(horizon_months)
        result = pd.DataFrame(
            {
                "ds": forecast["ds"].dt.strftime("%Y-%m"),
                "yhat": forecast["yhat"],
                "yhat_lower": forecast["yhat_lower"],
                "yhat_upper": forecast["yhat_upper"],
            }
        )
        engine_used = "prophet"

    elif FORECAST_ENGINE == "statsmodels_holt_winters":
        y = series["y"].astype(float)
        use_seasonal = len(y) >= 24 and (y - y.mean()).abs().sum() > 0
        try:
            if use_seasonal:
                model = ExponentialSmoothing(
                    y.values, trend="add", damped_trend=True, seasonal="mul", seasonal_periods=12
                ).fit(optimized=True)
            else:
                model = ExponentialSmoothing(y.values, trend="add", damped_trend=True).fit(optimized=True)
            fitted = model.fittedvalues
            residuals = y.values[: len(fitted)] - fitted
            resid_std = float(np.nanstd(residuals))
            future_vals = np.asarray(model.forecast(horizon_months), dtype=float)
            last_dates = pd.date_range(series["ds"].iloc[-1], periods=horizon_months + 1, freq="MS")[1:]
            widening = np.sqrt(np.arange(1, horizon_months + 1))
            result = pd.DataFrame(
                {
                    "ds": last_dates.strftime("%Y-%m"),
                    "yhat": future_vals,
                    "yhat_lower": future_vals - z * resid_std * widening,
                    "yhat_upper": future_vals + z * resid_std * widening,
                }
            )
            engine_used = "holt_winters_damped"
        except Exception as exc:
            log.warning("Holt-Winters failed (%s); falling back to linear trend", exc)
            x = np.arange(len(y))
            slope, intercept = np.polyfit(x, y.values.astype(float), 1)
            future_x = np.arange(len(y), len(y) + horizon_months)
            future_vals = intercept + slope * future_x
            resid_std = float(np.std(y.values - (intercept + slope * x))) or abs(float(y.mean())) * 0.05
            last_dates = pd.date_range(series["ds"].iloc[-1], periods=horizon_months + 1, freq="MS")[1:]
            widening = np.sqrt(np.arange(1, horizon_months + 1))
            result = pd.DataFrame(
                {
                    "ds": last_dates.strftime("%Y-%m"),
                    "yhat": future_vals,
                    "yhat_lower": future_vals - z * resid_std * widening,
                    "yhat_upper": future_vals + z * resid_std * widening,
                }
            )
            engine_used = "linear_trend"
    else:
        return {"error": "No forecasting library available. Install statsmodels or prophet."}

    numeric_cols = ["yhat", "yhat_lower", "yhat_upper"]
    result[numeric_cols] = result[numeric_cols].clip(lower=-np.inf).round(2)
    return {
        "forecast": result,
        "engine": engine_used,
        "history": series.assign(ds=series["ds"].dt.strftime("%Y-%m")),
    }


METRIC_ALIASES = {
    "revenue": ["revenue", "income", "sales"],
    "total_expense": ["expense", "spend", "cost", "burn", "expenditure"],
    "net_profit": ["profit", "margin", "earnings", "ebitda", "pnl", "p&l"],
}


def detect_metric(prompt: str) -> str | None:
    lowered = prompt.lower()
    best: tuple[int, str] | None = None
    for canonical, aliases in METRIC_ALIASES.items():
        for alias in aliases:
            idx = lowered.find(alias)
            if idx != -1 and (best is None or idx < best[0]):
                best = (idx, canonical)
    return best[1] if best else None


def detect_horizon(prompt: str, default: int = 6) -> int:
    import re

    match = re.search(r"(\d{1,2})\s*(month|mo\b)", prompt.lower())
    if match:
        return min(int(match.group(1)), 18)
    if "quarter" in prompt.lower():
        return 3
    if "half" in prompt.lower():
        return 6
    if "year" in prompt.lower():
        return 12
    return default
