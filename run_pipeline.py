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


def _ingest(args: argparse.Namespace) -> dict:
    return ingest.run_full_ingest(
        stream_days=args.days,
        txns_per_day=settings.SIM_TXNS_PER_DAY,
        include_ocr=not args.no_ocr,
        target_rows=args.rows,
        history_months=args.months,
        reset=args.reset,
    )


def cmd_full(args: argparse.Namespace) -> None:
    log.info("Step 1/3 - Ingesting sources into bronze (%s rows over %s months%s)",
             f"{args.rows or settings.SIM_TARGET_FACT_ROWS:,}", args.months or settings.SIM_HISTORY_MONTHS,
             ", fresh lake" if args.reset else "")
    summary = _ingest(args)
    log.info("Ingest summary: %s", json.dumps(summary, default=str))

    log.info("Step 2/3 - Running notebooks (bronze -> silver -> gold) into staging, publishing on green tests")
    meta = transform.run_transforms(save_outputs=args.save_outputs)
    log.info("Gold tables: %s", json.dumps(meta["gold_row_counts"]))

    if not args.no_sync:
        log.info("Step 3/3 - Syncing to external warehouses (skips unconfigured targets)")
        sync.sync_all_warehouses()

    log.info("Done. Warehouse: %s", settings.DUCKDB_PATH)


def cmd_ingest(args: argparse.Namespace) -> None:
    print(json.dumps(_ingest(args), indent=2, default=str))


def cmd_transform(args: argparse.Namespace) -> None:
    meta = transform.run_transforms(save_outputs=args.save_outputs)
    print(json.dumps(meta, indent=2, default=str))


def cmd_sync(_args: argparse.Namespace) -> None:
    results = sync.sync_all_warehouses()
    print(json.dumps(results, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="AI Finance Lakehouse pipeline runner")
    sub = parser.add_subparsers(dest="command")

    def ingest_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--rows", type=int, default=None, help="target fact rows (default SIM_TARGET_FACT_ROWS=2M)")
        p.add_argument("--months", type=int, default=None, help="months of history (default SIM_HISTORY_MONTHS=24)")
        p.add_argument("--days", type=int, default=14, help="days of fresh events to publish to the Kafka topic")
        p.add_argument("--no-ocr", action="store_true")
        p.add_argument("--reset", action="store_true", help="wipe bronze + generated drops before ingesting")

    p_full = sub.add_parser("full", help="ingest -> notebooks -> sync")
    ingest_args(p_full)
    p_full.add_argument("--no-sync", action="store_true")
    p_full.add_argument("--save-outputs", action="store_true", help="write cell outputs back into the notebooks")
    p_full.set_defaults(func=cmd_full)

    p_ing = sub.add_parser("ingest", help="generate sources and land them in bronze")
    ingest_args(p_ing)
    p_ing.set_defaults(func=cmd_ingest)

    p_tr = sub.add_parser("transform", help="run the silver/gold notebooks against bronze")
    p_tr.add_argument("--save-outputs", action="store_true", help="write cell outputs back into the notebooks")
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
