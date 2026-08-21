from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from config import settings
from src.warehouses.cloud_stores import sync_bigquery, sync_mssql
from src.warehouses.duckdb_store import sync_motherduck
from src.warehouses.postgres_store import sync_postgres

log = logging.getLogger(__name__)


def sync_all_warehouses() -> dict[str, Any]:
    results = {
        "local_duckdb": {"status": "ok", "path": str(settings.DUCKDB_PATH)},
        "motherduck": sync_motherduck(),
        "postgres": sync_postgres(),
        "bigquery": sync_bigquery(),
        "mssql": sync_mssql(),
    }
    meta = {
        "last_sync_utc": datetime.now(timezone.utc).isoformat(),
        "results": results,
    }
    (settings.DATA_DIR / "sync_meta.json").write_text(
        json.dumps(meta, indent=2, default=str), encoding="utf-8"
    )
    for name, res in results.items():
        log.info("Warehouse %-14s -> %s", name, res.get("status"))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    sync_all_warehouses()
