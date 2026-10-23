"""Visual system for the Streamlit app: palette, Plotly template, CSS and small formatting helpers.

Categorical slots are the validated dark-mode reference palette (checked against the #131824
card surface: lightness band, chroma, CVD and normal-vision separation, 3:1 contrast all pass).
Colors follow entities in a fixed order and are never cycled.
"""
from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio

SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, RED = SERIES
SEQ_BLUE = ["#184f95", "#1c5cab", "#2a78d6", "#5598e7", "#86b6ef"]  # ordinal, dark-surface safe
DIVERGING = {"neg": RED, "mid": "#383835", "pos": BLUE}
STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}

INK = "#f0efec"
INK_2 = "#c3c2b7"
MUTED = "#898781"
GRID = "#262b38"
AXIS = "#383835"
SURFACE = "#131824"
PAGE = "#0b0f19"

# fixed entity -> color maps so a filter never repaints survivors
CATEGORY_COLORS = {"Revenue": BLUE, "OPEX": ORANGE, "COGS": AQUA, "CAPEX": YELLOW, "Other": MAGENTA,
                   "Unclassified": MUTED}
SEGMENT_COLORS = {"Enterprise": BLUE, "Mid-Market": ORANGE, "SMB": AQUA, "Unassigned": MUTED}
REGION_COLORS = {"NA": BLUE, "EMEA": ORANGE, "Unknown": MUTED, "APAC": AQUA, "LATAM": YELLOW}
CHANNEL_COLORS = {"erp_backfill": BLUE, "kafka_stream": ORANGE, "pos_legacy": AQUA, "file_upload": YELLOW}
AGING_ORDER = ["Current", "1-30", "31-60", "61-90", "90+"]
AGING_COLORS = dict(zip(AGING_ORDER, SEQ_BLUE))


def register_template() -> None:
    template = go.layout.Template()
    template.layout = go.Layout(
        font=dict(family="Inter, system-ui, -apple-system, Segoe UI, sans-serif", color=INK_2, size=12),
        title=dict(font=dict(size=15, color=INK), x=0, xanchor="left", y=0.975, yref="container"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        colorway=SERIES,
        margin=dict(l=8, r=8, t=74, b=8),
        hoverlabel=dict(bgcolor="#1d2333", bordercolor="#3a4258", font=dict(color=INK, size=12)),
        hovermode="closest",
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0, font=dict(color=INK_2),
                    bgcolor="rgba(0,0,0,0)", title_text=""),
        xaxis=dict(gridcolor="rgba(0,0,0,0)", linecolor=AXIS, tickcolor=AXIS, zeroline=False,
                   tickfont=dict(color=MUTED), title=dict(font=dict(color=MUTED))),
        yaxis=dict(gridcolor=GRID, linecolor="rgba(0,0,0,0)", zeroline=False, tickfont=dict(color=MUTED),
                   title=dict(font=dict(color=MUTED))),
        bargap=0.28,
        bargroupgap=0.08,
    )
    template.data.bar = [go.Bar(marker=dict(line=dict(width=0), cornerradius=4))]
    template.data.scatter = [go.Scatter(line=dict(width=2), marker=dict(size=8))]
    template.data.pie = [go.Pie(marker=dict(line=dict(color=SURFACE, width=2)), sort=False)]
    template.data.treemap = [go.Treemap(marker=dict(line=dict(color=SURFACE, width=2)))]
    pio.templates["lakehouse"] = template
    pio.templates.default = "lakehouse"


def money(v: float | None, digits: int = 1) -> str:
    if v is None:
        return "—"
    sign = "-" if v < 0 else ""
    v = abs(float(v))
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if v >= div:
            return f"{sign}${v / div:.{digits}f}{suffix}"
    return f"{sign}${v:,.0f}"


def count(v: float | None) -> str:
    if v is None:
        return "—"
    v = float(v)
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 1e4:
        return f"{v / 1e3:.1f}K"
    return f"{v:,.0f}"


def pct_delta(cur: float | None, prev: float | None) -> str | None:
    if cur is None or prev in (None, 0):
        return None
    return f"{(cur - prev) / abs(prev) * 100:+.1f}%"


CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Space+Grotesk:wght@600;700&display=swap');
html, body, [class*="css"], .stMarkdown, .stText {{ font-family: 'Inter', system-ui, sans-serif; }}
.stApp {{ background: radial-gradient(1200px 500px at 15% -10%, rgba(57,135,229,.10), transparent 60%),
                     radial-gradient(900px 400px at 95% 0%, rgba(144,133,233,.10), transparent 60%), {PAGE}; }}
[data-testid="stHeader"] {{ background: transparent; }}
.block-container {{ padding-top: 2.2rem; padding-bottom: 3rem; max-width: 1480px; }}
h1, h2, h3 {{ font-family: 'Space Grotesk', 'Inter', sans-serif !important; letter-spacing: -.02em; }}
[data-testid="stSidebar"] {{ background: linear-gradient(180deg, #0f1422 0%, #0b0f19 100%); border-right: 1px solid #1e2433; }}
[data-testid="stSidebar"] [role="radiogroup"] label {{ padding: 7px 10px; border-radius: 10px; margin-bottom: 2px;
    transition: background .15s; width: 100%; }}
[data-testid="stSidebar"] [role="radiogroup"] label:hover {{ background: rgba(255,255,255,.04); }}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {{ background: rgba(57,135,229,.16);
    box-shadow: inset 0 0 0 1px rgba(57,135,229,.45); }}

[data-testid="stMetric"] {{ background: linear-gradient(160deg, #161c2b 0%, {SURFACE} 100%);
    border: 1px solid #232a3b !important; border-radius: 16px; padding: 14px 16px 10px; transition: border-color .2s, transform .2s; }}
[data-testid="stMetric"]:hover {{ border-color: #3a4a6e !important; transform: translateY(-1px); }}
[data-testid="stMetricLabel"] p {{ color: {MUTED} !important; font-size: .78rem !important; text-transform: uppercase;
    letter-spacing: .06em; font-weight: 600; }}
[data-testid="stMetricValue"] {{ font-family: 'Space Grotesk', sans-serif; font-size: 1.75rem !important; color: {INK}; }}

[class*="st-key-card_"] {{ background: linear-gradient(170deg, #151b2a 0%, {SURFACE} 100%); border: 1px solid #232a3b;
    border-radius: 16px; padding: 10px 14px 6px; }}
.stPlotlyChart {{ border-radius: 12px; }}
div[data-testid="stExpander"] details {{ border-radius: 12px; border-color: #232a3b; background: {SURFACE}; }}
[data-testid="stDataFrame"] {{ border-radius: 12px; overflow: hidden; }}

.hero {{ padding: 6px 0 18px; }}
.hero .eyebrow {{ font: 600 .74rem 'Inter'; letter-spacing: .14em; text-transform: uppercase; color: {AQUA}; }}
.hero h1 {{ font-size: 2.35rem; margin: .2rem 0 .35rem; line-height: 1.12; }}
.hero h1 .grad {{ background: linear-gradient(95deg, #5aa7ff, #9085e9 55%, #e87ba4); -webkit-background-clip: text;
    background-clip: text; color: transparent; }}
.hero p {{ color: {INK_2}; max-width: 860px; margin: 0; }}
.chip {{ display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px; border-radius: 999px; font-size: .74rem;
    border: 1px solid #2a3245; color: {INK_2}; background: rgba(255,255,255,.03); margin: 0 6px 6px 0; }}
.chip .dot {{ width: 7px; height: 7px; border-radius: 50%; background: {STATUS['good']}; box-shadow: 0 0 8px {STATUS['good']}; }}
.flow {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: stretch; margin: 6px 0 4px; }}
.flow .step {{ flex: 1 1 150px; background: {SURFACE}; border: 1px solid #232a3b; border-radius: 14px; padding: 12px 14px; position: relative; }}
.flow .step b {{ display: block; color: {INK}; font-size: .92rem; margin-bottom: 3px; }}
.flow .step span {{ color: {MUTED}; font-size: .78rem; line-height: 1.35; display: block; }}
.flow .step i {{ position: absolute; top: 0; left: 14px; right: 14px; height: 2px; border-radius: 2px; }}
.callout {{ background: linear-gradient(160deg, #1a1f2e, {SURFACE}); border: 1px solid #2c3550; border-radius: 16px;
    padding: 16px 18px; color: {INK_2}; }}
.callout b {{ color: {INK}; }}
.callout code {{ color: #86b6ef; }}
.section-note {{ color: {MUTED}; font-size: .8rem; margin: -6px 0 8px; }}
</style>
"""
