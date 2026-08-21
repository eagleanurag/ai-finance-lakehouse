# ðŸ“Š AI Finance Lakehouse

**Enterprise-style, multi-source, multi-warehouse data platform with a GenAI financial analyst â€” running entirely on free tiers.**

Streaming + batch ingestion â†’ medallion lakehouse (Bronze/Silver/Gold) â†’ star schema warehouse â†’ live BI dashboard â†’ LLM chat that writes SQL and forecasts revenue. Every service is either forever-free or 12-month free tier, and the whole thing runs in **offline demo mode with zero accounts** out of the box.

---

## Architecture

```
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ SOURCES â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  ðŸ“¡ Streaming txns       ðŸ“ CSV/Excel uploads     ðŸ¢ SharePoint/Graph    â”‚
â”‚  (Kafka / Upstash REST   (Finance team budget     (Master data sync)     â”‚
â”‚   or local file broker)   exports)                 ðŸ§¾ Invoice images+OCR â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
           â–¼                         â–¼                       â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ BRONZE LAKE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚      Raw JSONL partitions â†’ local lake dir / Azure Blob (ADLS Gen2)     â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                   â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ SILVER (DuckDB SQL transforms) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚          typed Â· deduplicated Â· standardized entity tables              â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                   â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ GOLD STAR SCHEMA + KPIs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  dim_date Â· dim_department Â· dim_gl_account Â· dim_vendor                â”‚
â”‚  fact_transactions Â· fact_budget                                        â”‚
â”‚  kpi_monthly_actual_vs_budget Â· monthly_financials                      â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
           â–¼                               â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ BI SERVING â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”  â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€ AI ANALYST â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  Power BI publish-to-web embed â”‚  â”‚  Multi-provider router w/ fallback: â”‚
â”‚  + native Plotly KPI app       â”‚  â”‚  Groq â†’ Gemini â†’ Mistral â†’ GitHub   â”‚
â”‚                                â”‚  â”‚  Tools: Text-to-SQL Â· Forecasting   â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜  â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
```

## Highlights

- **4 source connector types** â€” Kafka streaming events (Upstash REST), CSV/Excel file drops, SharePoint/MS Graph sync, and imageâ†’text OCR invoice extraction (RapidOCR/Tesseract)
- **Medallion architecture** â€” immutable bronze partitions, deduplicated typed silver, gold star schema built from versioned SQL models executed by a DuckDB runner
- **Multi-warehouse sync** â€” one command pushes gold tables to Neon Postgres, BigQuery Sandbox, MotherDuck, and Azure SQL (each auto-skips when unconfigured)
- **LLM gateway with automatic failover** â€” Text-to-SQL answers survive rate limits by shifting Groq â†’ Gemini â†’ Mistral â†’ GitHub Models; manual model selection included
- **Guarded Text-to-SQL** â€” schema-aware prompting, SELECT-only sanitizer, self-repair retry loop, enforced LIMITs
- **Statistical forecasting** â€” Prophet if installed, otherwise damped Holt-Winters on monthly aggregates, rendered with confidence bands
- **Public showcase** â€” Streamlit Community Cloud hosts the dashboard + chat; recruiters need zero logins

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

Copy `.env.example` to `.env` and fill in only what you use. **Every key is optional** â€” unconfigured layers degrade gracefully.

| Layer | Service (free tier) | Env keys |
|---|---|---|
| Streaming | [Upstash Kafka](https://upstash.com) â€” forever free, 10k msg/day | `UPSTASH_KAFKA_REST_URL` `_USERNAME` `_PASSWORD` |
| Bronze lake | Azure Blob / ADLS Gen2 â€” 5 GB always free | `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_CONTAINER` |
| OLAP warehouse | [BigQuery Sandbox](https://cloud.google.com/bigquery/docs/sandbox) â€” no card, 1 TB query/mo | `BIGQUERY_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS` |
| OLTP serving | [Neon](https://neon.tech) / Supabase Postgres â€” free serverless | `POSTGRES_DSN` |
| Serverless OLAP | [MotherDuck](https://motherduck.com) â€” 10 GB free plan | `MOTHERDUCK_TOKEN` |
| MS SQL | Azure SQL DB â€” 12 months free | `MSSQL_DSN` (+ `pip install pyodbc`) |
| SharePoint | MS Graph app registration | `SHAREPOINT_TENANT_ID` `_CLIENT_ID` `_CLIENT_SECRET` `_SITE_HOST` `_DRIVE_ID` |
| LLM #1 | [Groq Cloud](https://console.groq.com) â€” free dev tier, fastest TtS engine | `GROQ_API_KEY` |
| LLM #2 | [Google AI Studio](https://aistudio.google.com) â€” free Gemini API | `GEMINI_API_KEY` |
| LLM #3 | [Mistral La Plateforme](https://console.mistral.ai) â€” free experiment tier | `MISTRAL_API_KEY` |
| LLM #4 | [GitHub Models](https://github.com/marketplace/models) â€” free with GitHub account | `GITHUB_MODELS_TOKEN` |
| BI | Power BI Desktop (free) â†’ publish-to-web URL | `POWERBI_EMBED_URL` |

### How the AI failover works

```
prompt â”€â”€â–º router("auto")
             â”œâ”€ 1. Groq gpt-oss-120b ....... HTTP 429? â”€â”€â”
             â”œâ”€ 2. Gemini 2.0 Flash .......... on error â”€â”€â”¤ retries next provider
             â”œâ”€ 3. Mistral Small ............. on error â”€â”€â”¤
             â””â”€ 4. GitHub Models gpt-4o-mini . on error â”€â”€â”˜
                          â–¼
        response + full attempt trace shown in the UI ("ðŸ”€ Router trace")
```

Order is configurable via `LLM_PROVIDER_ORDER`; the sidebar dropdown pins any single provider manually. The router treats 401/402/403/408/429/5xx/timeouts as failover triggers.

## Deploy the public demo (Streamlit Community Cloud)

1. Push this repo to GitHub
2. On [share.streamlit.io](https://share.streamlit.io): New app â†’ pick repo â†’ main file = `app/app.py`
3. Add secrets in the app settings (any subset):

```toml
GROQ_API_KEY = "gsk_..."
GEMINI_API_KEY = "..."
POWERBI_EMBED_URL = "https://app.powerbi.com/view?r=..."
```

First visitor triggers the bootstrap (~15 s): data simulates relative to *today*, builds bronzeâ†’gold, then serves. Subsequent loads are instant.

## Nightly orchestration (GitHub Actions)

`.github/workflows/pipeline.yml` runs daily at 02:00 UTC: ingest new simulated days â†’ transform â†’ sync configured warehouses (so a published Power BI report refreshes from Neon/BigQuery). Add repo secrets matching the table above; set variable `ENABLE_WAREHOUSE_SYNC=true` once warehouses are configured.

## Power BI publish-to-web

1. Open Power BI Desktop (free) â†’ Get Data â†’ connect to your Neon Postgres or BigQuery gold tables (or the exported CSVs from the Data Explorer tab)
2. Build the report â†’ Publish to Power BI Service
3. File â†’ Embed report â†’ **Publish to web (public)** â†’ copy link
4. Paste into the Dashboard tab's "Publish-to-web URL" box (or set `POWERBI_EMBED_URL`)

Requires a work/school email; a free [Microsoft 365 Developer Program](https://developer.microsoft.com/en-us/microsoft-365/dev-program) tenant works.

## Project structure

```
ai-finance-lakehouse/
â”œâ”€â”€ app/app.py                     # Streamlit showcase (dashboard, chat, explorer, ops)
â”œâ”€â”€ run_pipeline.py                # CLI: ingest | transform | sync | full
â”œâ”€â”€ config/settings.py             # env/secrets loader, paths, provider order
â”œâ”€â”€ src/sources/                   # simulator, Kafka client (REST+file broker),
â”‚                                  # file ingest, SharePoint/Graph, OCR, invoice renderer
â”œâ”€â”€ src/pipelines/                 # bronze landing (+Azure mirror), ingest orchestrator,
â”‚                                  # DuckDB transform runner, warehouse sync
â”œâ”€â”€ src/warehouses/                # MotherDuck, Neon Postgres, BigQuery, MSSQL adapters
â”œâ”€â”€ src/ai/                        # LLM fallback router, Text-to-SQL guard, forecaster, agent
â”œâ”€â”€ transform/sql/*.sql            # ordered silver/gold SQL models (11 models)
â”œâ”€â”€ data/                          # auto-generated lake, warehouse, sample invoices
â””â”€â”€ .github/workflows/pipeline.yml # nightly ETL cron
```

## Resume bullets this project demonstrates

- Designed a hybrid batch/streaming lakehouse (Kafka â†’ Bronze/Silver/Gold) with idempotent, partitioned raw storage and dimensional star-schema modeling
- Built vendor-agnostic LLM gateway with rate-limit-aware multi-provider failover and read-only guarded Text-to-SQL generation with self-correction loops
- Integrated statistical forecasting (Prophet/Holt-Winters) into an agentic analytics workflow with executive-grade narrative synthesis
- Delivered $0 cloud cost via layered free tiers (Upstash, Azure, GCP BigQuery Sandbox, Neon, MotherDuck, Streamlit Cloud, GitHub Actions) with graceful degradation when unconfigured

