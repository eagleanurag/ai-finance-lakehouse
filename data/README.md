# Data directory

Everything here is **auto-generated** at runtime — nothing needs to be committed or configured.

| Path | Produced by | Purpose |
|---|---|---|
| `lake/bronze/transactions/dt=…/source=…/` | Ingestion pipeline | Raw ledger events: ERP parquet backfill, Kafka JSONL (replay + drained stream), POS and adjustment CSV drops |
| `lake/bronze/{budgets,ap_invoices,fx_rates}/…` | Ingestion pipeline | Plan files (CSV), AP sub-ledger (parquet), FX rate feed (parquet) |
| `lake/bronze/masters/<entity>/dt=<snapshot>/…` | Ingestion pipeline | Dated SharePoint / CRM snapshots of departments, GL accounts, vendors, customers |
| `stream_outbox/*.jsonl` | Kafka producer fallback | Local file-based event broker queue when no Kafka cluster is configured |
| `sample_invoices/*.png` | `invoice_renderer` | Synthetic vendor invoices for the OCR ingestion demo |
| `sources_files/` | `seed_source_files()` | Finance-team drops: budget plans, manual adjustments, legacy POS extracts |
| `sources_files/sharepoint_export/` | `seed_source_files()` | Older ERP master-data exports (lower precedence than the API snapshots) |
| `warehouse.duckdb` | Notebook runner | Live warehouse: `silver`, `gold` and `audit` schemas (replaced only when gold tests pass) |
| `warehouse.staging.duckdb` | Notebook runner | Build in progress; left behind for debugging when a run fails |
| `transform_meta.json` / `sync_meta.json` | Pipeline runners | Last-run audit metadata shown in the app |

Delete this folder at any time — the next pipeline run (or first app load, via the nightly snapshot) rebuilds everything.
Bronze files are deterministic per month, so re-running ingestion overwrites instead of piling up copies; pass `--reset`
to start from an empty lake.
