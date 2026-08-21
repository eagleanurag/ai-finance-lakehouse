from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

import pandas as pd

from config import settings
from src.pipelines import bronze
from src.sources import file_ingest, ocr_ingest, sharepoint_ingest
from src.sources.invoice_renderer import render_sample_invoices
from src.sources.kafka_client import EventBroker, partition_path
from src.sources.simulator import (
    generate_budgets,
    generate_masters,
    generate_manual_adjustments,
    generate_transactions,
)

log = logging.getLogger(__name__)


def publish_stream(days: int, txns_per_day: int) -> int:
    broker = EventBroker()
    events = generate_transactions(days=days, txns_per_day=txns_per_day)
    sent = broker.produce(events)
    log.info("Published %d transaction events via %s", sent, broker.description)
    return sent


def drain_stream_to_bronze(max_messages: int = 20000) -> dict[str, Any]:
    broker = EventBroker()
    events = broker.consume_all(max_messages=max_messages)
    if not events:
        return {"events": 0}
    path = partition_path(settings.BRONZE_TXN_DIR, event_date=str(events[0].get("event_date", ""))[:10])
    with open(path, "w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event, default=str) + "\n")
    bronze._upload_to_azure(path)
    return {"events": len(events), "file": str(path.relative_to(settings.PROJECT_ROOT))}


def land_master_data() -> int:
    total = 0
    for name, records in generate_masters().items():
        bronze.land_records(records, settings.BRONZE_MASTER_DIR / name, source_tag="sharepoint_api")
        total += len(records)
    return total


def land_budgets(months: int = 18) -> int:
    budgets = generate_budgets(months=months)
    bronze.land_records(budgets, settings.BRONZE_BUDGET_DIR, source_tag="file_upload")
    return len(budgets)


def land_file_uploads() -> list[dict[str, Any]]:
    landed: list[dict[str, Any]] = []
    for batch in file_ingest.ingest_incoming_files():
        target = _dataset_dir(batch["dataset"])
        bronze.land_records(batch["records"], target[0], source_tag=target[1])
        landed.append({"source_file": batch["source_file"], "dataset": batch["dataset"], "rows": batch["rows"]})
    return landed


def land_sharepoint() -> list[dict[str, Any]]:
    mode = sharepoint_ingest.sharepoint_mode()
    results: list[dict[str, Any]] = []
    for batch in sharepoint_ingest.fetch_sharepoint_files():
        import pandas as pd

        df = pd.DataFrame(batch["records"])
        from src.sources.file_ingest import _detect_dataset
        from pathlib import Path as _Path

        dataset = _detect_dataset(_Path(batch["source_file"]), df)
        target = _dataset_dir(dataset)
        bronze.land_records(batch["records"], target[0], source_tag=f"sharepoint_{mode}")
        results.append({"source_file": batch["source_file"], "rows": batch["rows"], "mode": mode})
    return results


def _dataset_dir(dataset: str) -> tuple:
    if dataset == "budgets":
        return settings.BRONZE_BUDGET_DIR, "file_upload"
    if dataset == "transactions":
        return settings.BRONZE_TXN_DIR, "file_upload"
    return settings.BRONZE_MASTER_DIR / dataset, "sharepoint_api"


def seed_source_files() -> dict[str, int]:
    seeded = {}
    adj_path = settings.FILES_DIR / f"manual_adjustments_{date.today().strftime('%Y%m')}.csv"
    if not any(settings.FILES_DIR.glob("manual_adjustments_*.csv")):
        rows = generate_manual_adjustments()
        pd.DataFrame(rows).to_csv(adj_path, index=False)
        seeded["adjustments_csv"] = len(rows)
    if not any(settings.SHAREPOINT_EXPORT_DIR.glob("*.csv")):
        masters = generate_masters()
        for name, records in masters.items():
            out = settings.SHAREPOINT_EXPORT_DIR / f"{name}.csv"
            pd.DataFrame(records).to_csv(out, index=False)
            seeded[f"sharepoint_{name}"] = len(records)
    return seeded


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
    stream_days: int,
    txns_per_day: int | None = None,
    budget_months: int = 18,
    drain_stream: bool = True,
    include_ocr: bool = True,
) -> dict[str, Any]:
    per_day = txns_per_day or settings.SIM_TXNS_PER_DAY
    summary: dict[str, Any] = {}
    seeded = seed_source_files()
    if seeded:
        summary["seeded_source_files"] = seeded
    summary["masters"] = land_master_data()
    summary["budgets"] = land_budgets(months=budget_months)
    summary["published_events"] = publish_stream(days=stream_days, txns_per_day=per_day)
    if drain_stream:
        drained = drain_stream_to_bronze()
        summary["drained_events"] = drained.get("events", 0)
    uploads = land_file_uploads()
    if uploads:
        summary["file_uploads"] = uploads
    sp = land_sharepoint()
    if sp:
        summary["sharepoint"] = sp
    if include_ocr:
        invoices = land_invoices(render_first=True)
        summary["invoices_ocr"] = len(invoices)
    return summary
