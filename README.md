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
│  Raw parquet / CSV / JSONL partitions → local lake / Azure Blob (ADLS)  │
│  ERP · POS · Kafka · budgets · AP sub-ledger · FX · masters  (~2.4M rows)│
└──────────────────────────────────┬──────────────────────────────────────┘
                                   ▼
┌──────────── SILVER · notebooks/01_bronze_to_silver.ipynb ───────────────┐
│  CDC · dedupe · FX · SCD2 masters · quarantine · audit.dq_results       │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   ▼
┌───────── GOLD STAR SCHEMA · notebooks/02_silver_to_gold.ipynb ──────────┐
│  dim_date · dim_department(+history) · dim_gl_account · dim_vendor      │
│  dim_customer · fact_transactions (2M) · fact_budget · fact_ap_invoices │
│  KPI tables · 39 production tests gate the publish                      │
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
- **2M-row production-scale data with real defects** — 8 synthetic sources (ERP, POS, Kafka, budgets, AP sub-ledger, FX, SharePoint, CRM) with ~20 deliberately injected data-engineering problems
- **Medallion architecture in notebooks** — `01_bronze_to_silver.ipynb` cleans, conforms and quarantines; `02_silver_to_gold.ipynb` builds the star schema and runs 39 production tests that gate publishing
- **Blue/green warehouse builds** — notebooks write to a staging database that replaces the live one only when every gold test passes
- **Multi-warehouse sync** — one command pushes gold tables to Neon Postgres, BigQuery Sandbox, MotherDuck, and Azure SQL (each auto-skips when unconfigured)
- **LLM gateway with automatic failover** — Text-to-SQL answers survive rate limits by shifting Groq → Gemini → Mistral → OpenRouter; manual model selection included
- **Guarded Text-to-SQL** — schema-aware prompting, SELECT-only sanitizer, self-repair retry loop, enforced LIMITs
- **Statistical forecasting** — Prophet if installed, otherwise damped Holt-Winters on monthly aggregates, rendered with confidence bands
- **Public showcase** — Streamlit Community Cloud hosts the dashboard + chat; recruiters need zero logins

## Quickstart (no accounts needed)

```bash
pip install -r requirements.txt
python run_pipeline.py full --no-sync    # generate 2M rows -> bronze -> notebooks -> gold (~3 min)
streamlit run app/app.py                 # open http://localhost:8501
```

Useful flags: `--rows 200000` for a quick build, `--reset` to wipe bronze first, `--save-outputs` to write
results back into the notebooks. The notebooks also run interactively in Jupyter/VS Code from `notebooks/`.

## Data: sources, scale and edge cases

| Source | Bronze format | Rows (default) | Defects injected on purpose |
|---|---|---|---|
| ERP ledger backfill | Parquet, monthly | ~1.9M | CDC updates & deletes, redelivered duplicates, schema drift (new columns), NaN / zero / ×1000 amounts, null ids, future-dated events, late arrivals |
| Kafka stream + replay | JSONL envelopes | ~40k | malformed lines, schema v1 `amt` vs v2 `amount`, epoch-millis and `+05:30` timestamps |
| Legacy POS | CSV per month | ~80k | renamed columns, `MM/DD/YYYY`, `"$1,234.50"` / `N/A` amounts, lower-case currency, voided receipts |
| Manual adjustments | CSV | 1.5k | rows uploaded twice |
| Budgets | CSV, 2 layouts | ~55k | `2025-01` / `Jan-2025` / broken periods, formatted amounts, resubmitted files, negative and blank lines, unknown departments |
| AP sub-ledger | Parquet | ~190k | status changes as CDC, duplicate billing, due date before invoice date, missing vendor, overpayments, credit notes |
| FX rates | Parquet | ~5k | weekend/holiday gaps, null and zero rates, two overlapping providers |
| Masters (SharePoint, CRM) | Parquet + CSV snapshots | 41 depts · 60 GL · 5.1k vendors · 51k customers | SCD2 changes, near-duplicate vendors and customers, code / country / category spellings, test accounts |

Across all channels, codes arrive as `ENG-NA`, `eng-na`, `" ENG-NA "`, `ENG_NA` or `ENGNA`, amounts come in 7
currencies with mixed casing, and 10% of transaction types are misspelled. Scale is set by `SIM_TARGET_FACT_ROWS`
(default 2,000,000) and `SIM_HISTORY_MONTHS` (24).

### Silver: `notebooks/01_bronze_to_silver.ipynb`
Lands every channel into one shape, parses all formats, applies CDC (latest `event_version` wins, deletes remove the
row), removes redelivered duplicates, converts to USD with gap-filled ECB rates, resolves vendors and customers
through alias maps, builds SCD2 departments and customers, and quarantines hard failures (unparseable values,
future dates, invalid currency, missing FX, statistical outliers, duplicate invoices, …) into `audit.quarantine`.
Soft issues are kept and flagged so no money is lost. Every rule writes a row to `audit.dq_results`.

### Gold: `notebooks/02_silver_to_gold.ipynb`
Star schema with conformed dimensions (`-1` unknown / `-2` not-applicable members, inferred members for unregistered
vendors), `fact_transactions` (~2M), `fact_budget` (all plan versions, latest flagged), `fact_ap_invoices` with aging,
and KPI tables (month-to-date budgets prorated). 39 tests check keys, foreign keys, reconciliation to silver to the
cent and freshness; any `error` stops the publish.

Optional extras:

```bash
pip install rapidocr-onnxruntime     # enables the OCR invoice pipeline
pip install "psycopg[binary]" google-cloud-bigquery azure-storage-blob pyodbc prophet
```

## Configuration

Copy `.env.example` to `.env` and fill in only what you use. **Every key is optional** — unconfigured layers degrade gracefully.

| Layer | Service (free tier) | Env keys |
|---|---|---|
| Streaming | [Aiven Apache Kafka](https://aiven.io/free-kafka) — forever-free plan, real Kafka protocol over SSL, no card | `KAFKA_BOOTSTRAP` + client cert as files in `data/secrets/` or as secret text `KAFKA_CA_PEM` `KAFKA_ACCESS_CERT` `KAFKA_ACCESS_KEY` (or SASL `KAFKA_USERNAME` `KAFKA_PASSWORD`) |
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

### Live streaming

The app runs one always-on producer/consumer per process (`src/pipelines/live_stream.py`): every few seconds it
produces POS/ERP events, consumes them back from the broker, lands them in bronze (`source=kafka_live`, picked up by
the next notebook run) and feeds the **⚡ Live Stream** page, which refreshes itself every 3 seconds. It uses Aiven
Kafka when the cluster is reachable and otherwise an embedded broker, re-checking Kafka every five minutes — note
that Aiven powers off idle free services, which removes their DNS name until you power them on again.
Tune with `LIVE_STREAM_INTERVAL` (seconds, default 3) or turn it off with `LIVE_STREAM_ENABLED=false`.

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

On a cold start the app downloads the warehouse snapshot that the nightly workflow publishes as a GitHub Release
asset (`warehouse.duckdb.gz`, ~110 MB, a few seconds). If no snapshot is published yet it builds the warehouse
itself, which takes several minutes on Streamlit's shared CPU. Override the source with `WAREHOUSE_SNAPSHOT_URL`.

## Nightly orchestration (GitHub Actions)

`.github/workflows/pipeline.yml` runs daily at 02:00 UTC: generate sources → run both notebooks → sync configured
warehouses → publish the warehouse snapshot to the `warehouse-latest` release. The website workflow then redeploys
with fresh numbers. Add repo secrets matching the table above; set variable `ENABLE_WAREHOUSE_SYNC=true` once
warehouses are configured. Free-tier OLTP targets (Neon, Azure SQL) only receive gold tables up to
`SYNC_MAX_ROWS` (250k) so the 2M-row fact stays within their storage limits; MotherDuck and BigQuery get everything.

## Power BI publish-to-web

1. Open Power BI Desktop (free) → Get Data → connect to your Neon Postgres or BigQuery gold tables (or the exported CSVs from the Data Explorer tab)
2. Build the report → Publish to Power BI Service
3. File → Embed report → **Publish to web (public)** → copy link
4. Paste into the Dashboard tab's "Publish-to-web URL" box (or set `POWERBI_EMBED_URL`)

Requires a work/school email; a free [Microsoft 365 Developer Program](https://developer.microsoft.com/en-us/microsoft-365/dev-program) tenant works.

## Project structure

```
ai-finance-lakehouse/
├── app/app.py                     # Streamlit showcase (dashboard, chat, data quality, explorer, ops)
├── run_pipeline.py                # CLI: ingest | transform | sync | full
├── config/settings.py             # env/secrets loader, paths, provider order
├── notebooks/                     # 01_bronze_to_silver, 02_silver_to_gold (the transformation pipeline)
├── src/sources/                   # 2M-row data generator, Kafka client (REST+file broker),
│                                  # file ingest, SharePoint/Graph, OCR, invoice renderer
├── src/pipelines/                 # bronze landing (+Azure mirror), ingest orchestrator,
│                                  # notebook runner (staging + publish), snapshot, warehouse sync
├── src/warehouses/                # MotherDuck, Neon Postgres, BigQuery, MSSQL adapters
├── src/ai/                        # LLM fallback router, Text-to-SQL guard, forecaster, agent
├── data/                          # auto-generated lake, warehouse, sample invoices
└── .github/workflows/             # nightly build + snapshot release, GitHub Pages site
```

## Resume bullets this project demonstrates

- Designed a hybrid batch/streaming lakehouse (Kafka → Bronze/Silver/Gold) with idempotent, partitioned raw storage and dimensional star-schema modeling
- Processed 2M+ transactions from 8 heterogeneous sources, handling CDC, late-arriving and duplicate events, schema drift, SCD2 master data and multi-currency conversion, with quarantine and auditable data-quality metrics
- Gated warehouse publishing on 39 automated data tests (key integrity, referential integrity, reconciliation to the cent) using blue/green staging builds
- Built vendor-agnostic LLM gateway with rate-limit-aware multi-provider failover and read-only guarded Text-to-SQL generation with self-correction loops
- Integrated statistical forecasting (Prophet/Holt-Winters) into an agentic analytics workflow with executive-grade narrative synthesis
- Delivered $0 cloud cost via layered free tiers (Aiven, Azure, GCP BigQuery Sandbox, Neon, MotherDuck, Streamlit Cloud, GitHub Actions) with graceful degradation when unconfigured
