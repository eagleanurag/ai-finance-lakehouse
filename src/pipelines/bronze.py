from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from config import settings

log = logging.getLogger(__name__)


def land_records(
    records: list[dict[str, Any]],
    bronze_dir: Path,
    event_date: str | None = None,
    fmt: str = "jsonl",
    source_tag: str = "batch",
) -> Path:
    settings.ensure_dirs()
    dt = event_date or date.today().isoformat()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    folder = bronze_dir / f"dt={dt}" / f"source={source_tag}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"part-{stamp}.{fmt}"
    if fmt == "jsonl":
        with open(path, "w", encoding="utf-8") as fh:
            for record in records:
                fh.write(json.dumps(record, default=str) + "\n")
    elif fmt == "parquet":
        pd.DataFrame(records).to_parquet(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    _upload_to_azure(path)
    return path


def _upload_to_azure(local_path: Path) -> str | None:
    if not settings.AZURE_STORAGE_CONNECTION_STRING:
        return None
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        log.warning("AZURE_STORAGE_CONNECTION_STRING set but azure-storage-blob not installed; skipping upload")
        return None
    container = settings.AZURE_CONTAINER
    rel = local_path.relative_to(settings.LAKE_DIR).as_posix()
    service = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
    try:
        container_client = service.get_container_client(container)
        if not container_client.exists():
            container_client.create_container()
        with open(local_path, "rb") as data:
            container_client.upload_blob(name=rel, data=data, overwrite=True)
    finally:
        service.close()
    log.info("Uploaded bronze file to Azure: %s/%s", container, rel)
    return f"azure://{container}/{rel}"


def iter_bronze_files(bronze_dir: Path) -> list[Path]:
    if not bronze_dir.exists():
        return []
    files: list[Path] = []
    for root, _dirs, names in os.walk(bronze_dir):
        for name in sorted(names):
            if name.endswith((".jsonl", ".json", ".parquet")):
                files.append(Path(root) / name)
    return sorted(files)


def read_bronze_file(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        with open(path, "r", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
    if suffix == ".json":
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, list) else [payload]
    if suffix == ".parquet":
        return pd.read_parquet(path).to_dict(orient="records")
    return []


def bronze_stats() -> dict[str, int]:
    return {
        "transactions": sum(len(read_bronze_file(f)) for f in iter_bronze_files(settings.BRONZE_TXN_DIR)),
        "invoices": sum(len(read_bronze_file(f)) for f in iter_bronze_files(settings.BRONZE_INVOICE_DIR)),
        "budgets": sum(len(read_bronze_file(f)) for f in iter_bronze_files(settings.BRONZE_BUDGET_DIR)),
        "masters": sum(len(read_bronze_file(f)) for f in iter_bronze_files(settings.BRONZE_MASTER_DIR)),
    }
