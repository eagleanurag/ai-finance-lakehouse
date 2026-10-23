"""Ship the built warehouse as a compressed artifact instead of rebuilding it everywhere.

CI builds the full 2M-row warehouse nightly and publishes ``warehouse.duckdb.gz`` (plus the run
metadata) as a GitHub Release asset. The Streamlit app downloads it on a cold start in seconds
and only falls back to building from sources when the download is unavailable.
"""
from __future__ import annotations

import gzip
import logging
import os
import shutil
import tempfile
from pathlib import Path

import duckdb
import requests

from config import settings

log = logging.getLogger(__name__)

SNAPSHOT_NAME = "warehouse.duckdb.gz"
META_NAME = "transform_meta.json"


def pack(out_dir: Path | None = None) -> Path:
    out_dir = out_dir or settings.DATA_DIR
    target = out_dir / SNAPSHOT_NAME
    with open(settings.DUCKDB_PATH, "rb") as src, gzip.open(target, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, length=8 << 20)
    log.info("Packed %s (%.0f MB)", target, target.stat().st_size / 1e6)
    return target


def fact_rows(path: Path) -> int:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute("SELECT count(*) FROM gold.fact_transactions").fetchone()[0]
    finally:
        con.close()


def download(base_url: str | None = None, timeout: int = 300) -> bool:
    """Fetch and install the published warehouse; returns False (leaving any local file alone) on failure."""
    base_url = (base_url or settings.WAREHOUSE_SNAPSHOT_URL).rstrip("/")
    if not base_url:
        return False
    settings.ensure_dirs()
    tmp_dir = Path(tempfile.mkdtemp(dir=settings.DATA_DIR))
    try:
        packed = tmp_dir / SNAPSHOT_NAME
        with requests.get(f"{base_url}/{SNAPSHOT_NAME}", stream=True, timeout=timeout) as resp:
            resp.raise_for_status()
            with open(packed, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8 << 20):
                    fh.write(chunk)
        unpacked = tmp_dir / settings.DUCKDB_PATH.name
        with gzip.open(packed, "rb") as src, open(unpacked, "wb") as dst:
            shutil.copyfileobj(src, dst, length=8 << 20)
        rows = fact_rows(unpacked)
        os.replace(unpacked, settings.DUCKDB_PATH)
        meta = requests.get(f"{base_url}/{META_NAME}", timeout=30)
        if meta.ok:
            (settings.DATA_DIR / META_NAME).write_text(meta.text, encoding="utf-8")
        log.info("Installed warehouse snapshot from %s (%s fact rows)", base_url, f"{rows:,}")
        return True
    except (requests.RequestException, OSError, duckdb.Error) as exc:
        log.warning("Warehouse snapshot unavailable (%s): %s", base_url, exc)
        return False
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
