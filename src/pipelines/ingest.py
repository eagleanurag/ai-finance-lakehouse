from __future__ import annotations

import json
import logging
import time
from datetime import date
from typing import Any

import pandas as pd

from config import settings
from src.pipelines import bronze
from src.sources import file_ingest, ocr_ingest, sharepoint_ingest
from src.sources.invoice_renderer import render_sample_invoices
from src.sources.kafka_client import EventBroker, partition_path
from src.sources.simulator import FinanceDataGenerator

log = logging.getLogger(__name__)

MASTER_ENTITIES = ("departments", "gl_accounts", "vendors", "customers")


def make_generator(target_rows: int | None = None, history_months: int | None = None) -> FinanceDataGenerator:
    return FinanceDataGenerator(
        history_months=history_months or settings.SIM_HISTORY_MONTHS,
        target_rows=target_rows or settings.SIM_TARGET_FACT_ROWS,
    )


def publish_stream(days: int, txns_per_day: int, gen: FinanceDataGenerator | None = None) -> int:
    broker = EventBroker()
    events = (gen or make_generator(target_rows=1000)).stream_events(days=days, per_day=txns_per_day)
    sent = broker.produce(events)
    log.info("Published %d transaction events via %s", sent, broker.description)
    return sent


def drain_stream_to_bronze(max_messages: int = 20000) -> dict[str, Any]:
    broker = EventBroker()
    events = broker.consume_all(max_messages=max_messages)
    if not events:
        return {"events": 0}
    path = partition_path(settings.BRONZE_TXN_DIR, event_date=date.today().isoformat())
    with open(path, "w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event, default=str) + "\n")
    bronze._upload_to_azure(path)
    return {"events": len(events), "file": str(path.relative_to(settings.PROJECT_ROOT))}


def land_history(gen: FinanceDataGenerator) -> dict[str, int]:
    """Backfill: ERP ledger extracts (parquet), Kafka replay (JSONL), POS drops (CSV), AP sub-ledger, FX."""
    counts = {"erp_backfill": 0, "kafka_stream_replay": 0, "pos_legacy_files": 0, "ap_invoices": 0}
    for month, parts in gen.transaction_months():
        dt = month.isoformat()
        bronze.write_partition(parts["erp"], settings.BRONZE_TXN_DIR, dt, "erp_backfill", "part-backfill.parquet")
        bronze.write_partition(parts["kafka"], settings.BRONZE_TXN_DIR, dt, "kafka_stream_replay", "part-replay.jsonl")
        if not parts["pos"].empty:
            parts["pos"].to_csv(settings.FILES_DIR / f"pos_legacy_{month:%Y%m}.csv", index=False)
        counts["erp_backfill"] += len(parts["erp"])
        counts["kafka_stream_replay"] += len(parts["kafka"])
        counts["pos_legacy_files"] += len(parts["pos"])
    for month, df in gen.ap_invoice_months():
        bronze.write_partition(df, settings.BRONZE_AP_DIR, month.isoformat(), "ap_system", "part-ap.parquet")
        counts["ap_invoices"] += len(df)
    fx = gen.fx_rates_raw()
    bronze.write_partition(fx, settings.BRONZE_FX_DIR, gen.end_date.isoformat(), "rates_feed", "part-fx.parquet")
    counts["fx_rates"] = len(fx)
    return counts


def land_master_data(gen: FinanceDataGenerator) -> dict[str, int]:
    """SharePoint/CRM API extracts: dated snapshots so silver can build SCD2 history."""
    counts: dict[str, int] = {}
    for snap_date, df in gen.department_snapshots():
        bronze.write_partition(df, settings.BRONZE_MASTER_DIR / "departments", snap_date.isoformat(),
                               "sharepoint_api", "part-snapshot.parquet")
        counts["departments"] = counts.get("departments", 0) + len(df)
    today = gen.end_date.isoformat()
    gl = gen.gl_accounts_raw()
    bronze.write_partition(gl, settings.BRONZE_MASTER_DIR / "gl_accounts", today, "sharepoint_api", "part-snapshot.parquet")
    vendors = gen.vendors_raw()
    bronze.write_partition(vendors, settings.BRONZE_MASTER_DIR / "vendors", today, "sharepoint_api", "part-snapshot.parquet")
    for snap_date, df in gen.customer_snapshots():
        bronze.write_partition(df, settings.BRONZE_MASTER_DIR / "customers", snap_date.isoformat(),
                               "crm_api", "part-snapshot.parquet")
        counts["customers"] = counts.get("customers", 0) + len(df)
    counts.update({"gl_accounts": len(gl), "vendors": len(vendors)})
    return counts


def seed_source_files(gen: FinanceDataGenerator) -> dict[str, int]:
    """Files the finance team and the old ERP drop into the shared folder (POS drops are written by land_history)."""
    seeded: dict[str, int] = {}
    for name, df in gen.budget_files():
        df.to_csv(settings.FILES_DIR / name, index=False)
        seeded[name] = len(df)
    adj = gen.manual_adjustments()
    adj.to_csv(settings.FILES_DIR / f"manual_adjustments_{gen.end_date:%Y%m}.csv", index=False)
    seeded["manual_adjustments"] = len(adj)
    gen.department_local_export().to_csv(settings.SHAREPOINT_EXPORT_DIR / "departments.csv", index=False)
    gen.vendors_local_export().to_csv(settings.SHAREPOINT_EXPORT_DIR / "vendors.csv", index=False)
    for stale in ("gl_accounts.csv",):  # superseded by the API extract
        (settings.SHAREPOINT_EXPORT_DIR / stale).unlink(missing_ok=True)
    return seeded


def land_file_uploads() -> list[dict[str, Any]]:
    landed: list[dict[str, Any]] = []
    for path in file_ingest.incoming_files():
        route = file_ingest.classify_drop(path)
        if route is None:
            log.warning("Unrecognised file drop %s - left in place", path.name)
            continue
        target_dir, dt, source_tag = route
        out = bronze.land_raw_file(file_ingest.as_csv(path), target_dir, dt, source_tag)
        landed.append({"source_file": path.name, "bronze": str(out.relative_to(settings.PROJECT_ROOT))})
    return landed


def land_sharepoint() -> list[dict[str, Any]]:
    mode = sharepoint_ingest.sharepoint_mode()
    today = date.today().isoformat()
    results: list[dict[str, Any]] = []
    for batch in sharepoint_ingest.fetch_sharepoint_files():
        entity = batch["source_file"].rsplit(".", 1)[0].lower()
        if entity not in MASTER_ENTITIES:
            log.warning("SharePoint file %s does not map to a master entity - skipped", batch["source_file"])
            continue
        df = pd.DataFrame(batch["records"])
        bronze.write_partition(df, settings.BRONZE_MASTER_DIR / entity, today, f"sharepoint_{mode}", f"{entity}.csv")
        results.append({"source_file": batch["source_file"], "rows": batch["rows"], "mode": mode})
    return results


def land_invoices(render_first: bool = False, max_invoices: int | None = None) -> list[dict[str, Any]]:
    if render_first and not any(settings.INVOICES_DIR.glob("*.png")):
        render_sample_invoices()
    engine = ocr_ingest.available_engine()
    if engine is None:
        log.warning("No OCR engine available; skipping invoice ingestion (pip install rapidocr-onnxruntime)")
        return []
    records = ocr_ingest.process_invoices_folder()
    for record in records:
        record["ocr_engine"] = engine
    if max_invoices:
        records = records[:max_invoices]
    if records:
        bronze.land_records(records, settings.BRONZE_INVOICE_DIR, source_tag=f"ocr_{engine}")
    return records


def run_full_ingest(
    stream_days: int = 14,
    txns_per_day: int | None = None,
    include_ocr: bool = True,
    target_rows: int | None = None,
    history_months: int | None = None,
    drain_stream: bool = True,
    reset: bool = False,
) -> dict[str, Any]:
    started = time.time()
    if reset:
        bronze.reset_lake()
    gen = make_generator(target_rows, history_months)
    summary: dict[str, Any] = {"window": f"{gen.history_start} -> {gen.end_date}"}
    summary["seeded_source_files"] = seed_source_files(gen)
    summary["masters"] = land_master_data(gen)
    summary["history"] = land_history(gen)
    summary["published_events"] = publish_stream(stream_days, txns_per_day or settings.SIM_TXNS_PER_DAY, gen)
    if drain_stream:
        summary["drained_events"] = drain_stream_to_bronze().get("events", 0)
    summary["file_uploads"] = len(land_file_uploads())
    summary["sharepoint_files"] = len(land_sharepoint())
    if include_ocr:
        summary["invoices_ocr"] = len(land_invoices(render_first=True))
    summary["seconds"] = round(time.time() - started, 1)
    return summary
