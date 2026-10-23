from __future__ import annotations

import re
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from config import settings

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def read_tabular(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        str(c).strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]
    return df


def classify_drop(path: Path) -> tuple[Path, str, str] | None:
    """Route a dropped finance file to (bronze entity dir, partition dt, source tag) by its name."""
    name = path.stem.lower()
    today = date.today().isoformat()
    if name.startswith("budget"):
        return settings.BRONZE_BUDGET_DIR, today, "file_upload"
    if name.startswith("pos_legacy"):
        period = re.search(r"(\d{4})(\d{2})", name)
        dt = f"{period.group(1)}-{period.group(2)}-01" if period else today
        return settings.BRONZE_TXN_DIR, dt, "pos_legacy"
    if name.startswith("manual_adjustments"):
        return settings.BRONZE_TXN_DIR, today, "file_upload"
    return None


def incoming_files() -> list[Path]:
    return [p for p in sorted(settings.FILES_DIR.glob("*")) if p.suffix.lower() in SUPPORTED_EXTENSIONS]


def as_csv(path: Path) -> Path:
    """Bronze stores spreadsheets as CSV so silver reads one text format; CSV drops pass through untouched."""
    if path.suffix.lower() == ".csv":
        return path
    out = path.with_suffix(".csv")
    read_tabular(path).to_csv(out, index=False)
    return out


def archive_processed(path: Path) -> Path:
    done_dir = settings.FILES_DIR / "_processed"
    done_dir.mkdir(parents=True, exist_ok=True)
    target = done_dir / path.name
    shutil.move(str(path), str(target))
    return target
