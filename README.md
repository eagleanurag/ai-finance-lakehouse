# 📊 AI Finance Lakehouse

**Enterprise-style, multi-source, multi-warehouse data platform with a GenAI financial analyst — running entirely on free tiers.**

Streaming + batch ingestion → medallion lakehouse (Bronze/Silver/Gold) → star schema warehouse → live BI dashboard → LLM chat that writes SQL and forecasts revenue. Every service is either forever-free or 12-month free tier, and the whole thing runs in **offline demo mode with zero accounts** out of the box.

---

## Architecture

```
┌──────────────────────────────── SOURCES ────────────────────────────────┐
│  📡 Streaming txns       📁 CSV/Excel uploads     🏢 SharePoint/Graph    │
│  (Apache Kafka SSL   (Finance team budget     (Master data sync)     │
│   or local file broker)   exports)                 🧾 Invoice images+OCR │
└──────────┬─────────────────────────┬───────────────────────┬────────────┘
           ▼                         ▼                       ▼
┌───────────────────────────── BRONZE LAKE ───────────────────────────────┐
│      Raw JSONL partitions → local lake dir / Azure Blob (ADLS Gen2)     │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   ▼
┌──────────────────── SILVER (DuckDB SQL transforms) ─────────────────────┐
│          typed · deduplicated · standardized entity tables              │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   ▼
┌────────────────────── GOLD STAR SCHEMA + KPIs ──────────────────────────┐
│  dim_date · dim_department · dim_gl_account · dim_vendor                │
│  fact_transactions · fact_budget                                        │
│  kpi_monthly_actual_vs_budget · monthly_financials                      │
└──────────┬───────────────────────────────┬──────────────────────────────┘
           ▼                               ▼
┌────────── BI SERVING ──────────┐  ┌───────── AI ANALYST ────────────────┐
│  Power BI publish-to-web embed │  │  Multi-provider router w/ fallback: │
│  + native Plotly KPI app       │  │  Groq → Gemini → Mistral → OpenRouter │
│                                │  │  Tools: Text-to-SQL · Forecasting   │
└────────────────────────────────┘  └─────────────────────────────────────┘
```

## Highlights

- **4 source connector types** — Kafka streaming events (Aiven, SASL_SSL), CSV/Excel file drops, SharePoint/MS Graph sync, and image→text OCR invoice extraction (RapidOCR/Tesseract)
- **Medallion architecture** — immutable bronze partitions, deduplicated typed silver, gold star schema built from versioned SQL models executed by a DuckDB runner
- **Multi-warehouse sync** — one command pushes gold tables to Neon Postgres, BigQuery Sandbox, MotherDuck, and Azure SQL (each auto-skips when unconfigured)
- **LLM gateway with automatic failover** — Text-to-SQL answers survive rate limits by shifting Groq → Gemini → Mistral → OpenRouter; manual model selection included
- **Guarded Text-to-SQL** — schema-aware prompting, SELECT-only sanitizer, self-repair retry loop, enforced LIMITs
- **Statistical forecasting** — Prophet if installed, otherwise damped Holt-Winters on monthly aggregates, rendered with confidence bands
- **Public showcase** — Streamlit Community Cloud hosts the dashboard + chat; recruiters need zero logins

## Quickstart (no accounts needed)

```bash
pip install -r requirements.txt
python run_pipeline.py full          # simulate sources -> Kafka -> bronze -> silver/gold
streamlit run app/app.py             # open http://localhost:8501
```

The pipeline prints per-step summaries; the app boots against `data/warehouse.duckdb`. If the warehouse is missing, the app rebuilds it automatically on first load.

Optional extras:

```bash
pip install rapidocr-onnxruntime     # enables the OCR invoice pipeline
pip install "psycopg[binary]" google-cloud-bigquery azure-storage-blob pyodbc prophet
```

## Configuration

Copy `.env.example` to `.env` and fill in only what you use. **Every key is optional** — unconfigured layers degrade gracefully.

| Layer | Service (free tier) | Env keys |
|---|---|---|
| Streaming | [Aiven Apache Kafka](https://aiven.io/free-kafka) — forever-free plan, real Kafka protocol over SSL, no card | `KAFKA_BOOTSTRAP` `KAFKA_USERNAME` `KAFKA_PASSWORD` |
| Bronze lake | Azure Blob / ADLS Gen2 — 5 GB always free | `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_CONTAINER` |
| OLAP warehouse | [BigQuery Sandbox](https://cloud.google.com/bigquery/docs/sandbox) — no card, 1 TB query/mo | `BIGQUERY_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS` |
| OLTP serving | [Neon](https://neon.tech) / Supabase Postgres — free serverless | `POSTGRES_DSN` |
| Serverless OLAP | [MotherDuck](https://motherduck.com) — 10 GB free plan | `MOTHERDUCK_TOKEN` |
| MS SQL | Azure SQL DB — 12 months free | `MSSQL_DSN` (+ `pip install pyodbc`) |
| SharePoint | MS Graph app registration | `SHAREPOINT_TENANT_ID` `_CLIENT_ID` `_CLIENT_SECRET` `_SITE_HOST` `_DRIVE_ID` |
| LLM #1 | [Groq Cloud](https://console.groq.com) — forever-free dev tier, fastest TtS engine | `GROQ_API_KEY` |
| LLM #2 | [Google AI Studio](https://aistudio.google.com) — free Gemini API tier | `GEMINI_API_KEY` |
| LLM #3 | [Mistral La Plateforme](https://console.mistral.ai) — free experiment tier | `MISTRAL_API_KEY` |
| LLM #4 | [OpenRouter](https://openrouter.ai/keys) — free model tier (`:free` models), no card | `OPENROUTER_API_KEY` |
| BI | Power BI Desktop (free) → publish-to-web URL | `POWERBI_EMBED_URL` |

### How the AI failover works

```
prompt ──► router("auto")
             ├─ 1. Groq gpt-oss-120b ........ HTTP 429? ──┐
             ├─ 2. Gemini Flash .............. on error ──┤ retries next provider
             ├─ 3. Mistral Small ............. on error ──┤
             └─ 4. OpenRouter nemotron :free . on error ──┘
                          ▼
        response + full attempt trace shown in the UI ("🔀 Router trace")
```

Order is configurable via `LLM_PROVIDER_ORDER`; the sidebar dropdown pins any single provider manually. The router treats 401/402/403/408/429/5xx/timeouts as failover triggers.

> ℹ️ Provider history: GitHub Models was retired by GitHub on July 30, 2026 and Cerebras moved its free tier behind a billing wall — both were dropped in favor of the four always-free providers above.

## Deploy the public demo (Streamlit Community Cloud)

1. Push this repo to GitHub
2. On [share.streamlit.io](https://share.streamlit.io): New app → pick repo → main file = `app/app.py`
3. Add secrets in the app settings (any subset):

```toml
GROQ_API_KEY = "gsk_..."
GEMINI_API_KEY = "..."
POWERBI_EMBED_URL = "https://app.powerbi.com/view?r=..."
```

First visitor triggers the bootstrap (~15 s): data simulates relative to *today*, builds bronze→gold, then serves. Subsequent loads are instant.

## Nightly orchestration (GitHub Actions)

`.github/workflows/pipeline.yml` runs daily at 02:00 UTC: ingest new simulated days → transform → sync configured warehouses (so a published Power BI report refreshes from Neon/BigQuery). Add repo secrets matching the table above; set variable `ENABLE_WAREHOUSE_SYNC=true` once warehouses are configured.

## Power BI publish-to-web

1. Open Power BI Desktop (free) → Get Data → connect to your Neon Postgres or BigQuery gold tables (or the exported CSVs from the Data Explorer tab)
2. Build the report → Publish to Power BI Service
3. File → Embed report → **Publish to web (public)** → copy link
4. Paste into the Dashboard tab's "Publish-to-web URL" box (or set `POWERBI_EMBED_URL`)

Requires a work/school email; a free [Microsoft 365 Developer Program](https://developer.microsoft.com/en-us/microsoft-365/dev-program) tenant works.

## Project structure

```
ai-finance-lakehouse/
├── app/app.py                     # Streamlit showcase (dashboard, chat, explorer, ops)
├── run_pipeline.py                # CLI: ingest | transform | sync | full
├── config/settings.py             # env/secrets loader, paths, provider order
├── src/sources/                   # simulator, Kafka client (REST+file broker),
│                                  # file ingest, SharePoint/Graph, OCR, invoice renderer
├── src/pipelines/                 # bronze landing (+Azure mirror), ingest orchestrator,
│                                  # DuckDB transform runner, warehouse sync
├── src/warehouses/                # MotherDuck, Neon Postgres, BigQuery, MSSQL adapters
├── src/ai/                        # LLM fallback router, Text-to-SQL guard, forecaster, agent
├── transform/sql/*.sql            # ordered silver/gold SQL models (11 models)
├── data/                          # auto-generated lake, warehouse, sample invoices
└── .github/workflows/pipeline.yml # nightly ETL cron
```

## Resume bullets this project demonstrates

- Designed a hybrid batch/streaming lakehouse (Kafka → Bronze/Silver/Gold) with idempotent, partitioned raw storage and dimensional star-schema modeling
- Built vendor-agnostic LLM gateway with rate-limit-aware multi-provider failover and read-only guarded Text-to-SQL generation with self-correction loops
- Integrated statistical forecasting (Prophet/Holt-Winters) into an agentic analytics workflow with executive-grade narrative synthesis
- Delivered $0 cloud cost via layered free tiers (Aiven, Azure, GCP BigQuery Sandbox, Neon, MotherDuck, Streamlit Cloud, GitHub Actions) with graceful degradation when unconfigured
