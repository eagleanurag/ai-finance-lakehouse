from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

OUT_DIR = PROJECT_ROOT / "docs" / "data"


def build() -> bool:
    import duckdb

    db_path = PROJECT_ROOT / "data" / "warehouse.duckdb"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    dictionary: dict[str, list] = {"silver": [], "gold": []}
    status = {
        "generated_utc": None,
        "warehouse_ready": False,
        "fact_rows": 0,
        "months_covered": 0,
        "channels": [],
        "gold_tables": {},
        "last_transform_utc": None,
    }

    meta_path = PROJECT_ROOT / "data" / "transform_meta.json"
    if meta_path.exists():
        try:
            status["last_transform_utc"] = json.loads(meta_path.read_text(encoding="utf-8")).get(
                "last_transform_utc"
            )
        except Exception:
            pass

    if db_path.exists():
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            cols = con.execute(
                """
                SELECT table_schema, table_name, column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema IN ('silver', 'gold')
                ORDER BY table_schema, table_name, ordinal_position
                """
            ).fetchall()
            for schema, table, column, dtype, _pos in cols:
                dictionary[schema].append(
                    {"table": table, "column": column, "type": dtype}
                )

            for schema in ("silver", "gold"):
                tables = sorted({r["table"] for r in dictionary[schema]})
                for t in tables:
                    n = con.execute(f'SELECT COUNT(*) FROM {schema}."{t}"').fetchone()[0]
                    status["gold_tables"][f"{schema}.{t}"] = n

            try:
                status["fact_rows"] = con.execute(
                    "SELECT COUNT(*) FROM gold.fact_transactions"
                ).fetchone()[0]
                status["months_covered"] = con.execute(
                    "SELECT COUNT(DISTINCT year_month) FROM gold.dim_date"
                ).fetchone()[0]
                status["channels"] = [
                    r[0]
                    for r in con.execute(
                        "SELECT DISTINCT source_channel FROM gold.fact_transactions ORDER BY 1"
                    ).fetchall()
                ]
                status["warehouse_ready"] = True
            except Exception:
                pass
        finally:
            con.close()

    status["generated_utc"] = datetime.now(timezone.utc).isoformat()

    (OUT_DIR / "data_dictionary.json").write_text(
        json.dumps(dictionary, indent=2), encoding="utf-8"
    )
    (OUT_DIR / "site_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")

    print(f"wrote {OUT_DIR / 'data_dictionary.json'} ({len(dictionary['gold'])} gold columns)")
    print(f"wrote {OUT_DIR / 'site_status.json'} (fact_rows={status['fact_rows']})")
    return True


if __name__ == "__main__":
    build()
