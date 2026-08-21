from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import settings
from src.pipelines import ingest, sync, transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("pipeline")


def cmd_full(args: argparse.Namespace) -> None:
    log.info("Step 1/4 - Ingesting sources (stream + files + masters%s)", " + OCR" if not args.no_ocr else "")
    summary = ingest.run_full_ingest(
        stream_days=args.days,
        txns_per_day=settings.SIM_TXNS_PER_DAY,
        budget_months=args.budget_months,
        include_ocr=not args.no_ocr,
    )
    log.info("Ingest summary: %s", json.dumps(summary, default=str))

    log.info("Step 2/4 - Running medallion transforms (bronze -> silver -> gold)")
    meta = transform.run_transforms()
    log.info("Gold tables: %s", json.dumps(meta["gold_row_counts"]))

    if not args.no_sync:
        log.info("Step 3/4 - Syncing to external warehouses (skips unconfigured targets)")
        sync.sync_all_warehouses()

    log.info("Step 4/4 - Done. Warehouse: %s", settings.DUCKDB_PATH)


def cmd_ingest(args: argparse.Namespace) -> None:
    summary = ingest.run_full_ingest(
        stream_days=args.days,
        txns_per_day=settings.SIM_TXNS_PER_DAY,
        budget_months=args.budget_months,
        include_ocr=not args.no_ocr,
    )
    print(json.dumps(summary, indent=2, default=str))


def cmd_transform(_args: argparse.Namespace) -> None:
    meta = transform.run_transforms()
    print(json.dumps(meta, indent=2, default=str))


def cmd_sync(_args: argparse.Namespace) -> None:
    results = sync.sync_all_warehouses()
    print(json.dumps(results, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="AI Finance Lakehouse pipeline runner")
    sub = parser.add_subparsers(dest="command")

    p_full = sub.add_parser("full", help="ingest -> transform -> sync")
    p_full.add_argument("--days", type=int, default=400)
    p_full.add_argument("--budget-months", type=int, default=18)
    p_full.add_argument("--no-ocr", action="store_true")
    p_full.add_argument("--no-sync", action="store_true")
    p_full.set_defaults(func=cmd_full)

    p_ing = sub.add_parser("ingest", help="run ingestion only")
    p_ing.add_argument("--days", type=int, default=30)
    p_ing.add_argument("--budget-months", type=int, default=6)
    p_ing.add_argument("--no-ocr", action="store_true")
    p_ing.set_defaults(func=cmd_ingest)

    p_tr = sub.add_parser("transform", help="rebuild silver/gold from bronze")
    p_tr.set_defaults(func=cmd_transform)

    p_sy = sub.add_parser("sync", help="push gold tables to configured warehouses")
    p_sy.set_defaults(func=cmd_sync)

    args = parser.parse_args()
    if not getattr(args, "command", None):
        parser.print_help()
        sys.exit(1)
    settings.ensure_dirs()
    args.func(args)


if __name__ == "__main__":
    main()
