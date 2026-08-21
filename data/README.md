# Data directory

Everything here is **auto-generated** at runtime — nothing needs to be committed or configured.

| Path | Produced by | Purpose |
|---|---|---|
| `lake/bronze/<entity>/dt=…/source=…/part-*.jsonl` | Ingestion pipeline | Raw immutable landing zone (mirrors ADLS Gen2 layout; optionally mirrored to Azure Blob) |
| `stream_outbox/*.jsonl` | Kafka producer fallback | Local file-based event broker queue when Upstash Kafka is not configured |
| `sample_invoices/*.png` | `invoice_renderer` | Synthetic vendor invoices for the OCR ingestion demo |
| `sources_files/` | `seed_source_files()` | Simulated finance-team CSV exports (budgets, manual adjustments) |
| `sources_files/sharepoint_export/` | `seed_source_files()` | Simulated SharePoint/ERP master-data exports |
| `warehouse.duckdb` | Transform runner | Local DuckDB gold warehouse (silver + star schema) |
| `transform_meta.json` / `sync_meta.json` | Pipeline runners | Last-run audit metadata shown in the app |

Delete this folder at any time — the next pipeline run (or first app load) rebuilds everything.
