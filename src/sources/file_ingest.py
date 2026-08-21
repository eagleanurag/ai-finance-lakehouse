from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

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


def ingest_incoming_files() -> list[dict[str, Any]]:
    landed: list[dict[str, Any]] = []
    for path in sorted(settings.FILES_DIR.glob("*")):
        if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            continue
        df = normalize_columns(read_tabular(path))
        landed.append(
            {
                "source_file": path.name,
                "dataset": _detect_dataset(path, df),
                "rows": len(df),
                "records": df.to_dict(orient="records"),
            }
        )
    return landed


def _detect_dataset(path: Path, df: pd.DataFrame) -> str:
    name = path.stem.lower()
    if "budget" in name or {"year_month", "dept_code", "account_code"} <= set(df.columns):
        return "budgets"
    if {"transaction_id", "event_date", "amount_usd"} <= set(df.columns):
        return "transactions"
    return name


def archive_processed(path: Path) -> Path:
    done_dir = settings.FILES_DIR / "_processed"
    done_dir.mkdir(parents=True, exist_ok=True)
    target = done_dir / path.name
    shutil.move(str(path), str(target))
    return target
