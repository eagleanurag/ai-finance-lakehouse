from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import requests


def check_kafka() -> dict:
    from config import settings

    if settings.KAFKA_MODE != "upstash_rest":
        return {"service": "Kafka", "ok": False, "detail": "UPSTASH_KAFKA_REST_URL not set — using local file broker"}
    try:
        resp = requests.post(
            f"{settings.UPSTASH_KAFKA_REST_URL}/produce/{settings.KAFKA_TOPIC}",
            data=json.dumps({"_health": "ping"}),
            auth=(settings.UPSTASH_KAFKA_REST_USERNAME, settings.UPSTASH_KAFKA_REST_PASSWORD),
            timeout=30,
        )
        resp.raise_for_status()
        return {"service": "Kafka", "ok": True, "detail": f"produced test event to topic '{settings.KAFKA_TOPIC}'"}
    except Exception as exc:
        return {"service": "Kafka", "ok": False, "detail": str(exc)[:200]}


def check_postgres() -> dict:
    from config import settings

    if not settings.POSTGRES_DSN:
        return {"service": "Postgres", "ok": False, "detail": "POSTGRES_DSN not set — gold tables live only in local DuckDB"}
    try:
        import psycopg

        with psycopg.connect(settings.POSTGRES_DSN, connect_timeout=15) as conn:
            version = conn.execute("SELECT version()").fetchone()[0]
            return {"service": "Postgres", "ok": True, "detail": version.split(",")[0]}
    except Exception as exc:
        return {"service": "Postgres", "ok": False, "detail": str(exc)[:200]}


def main() -> None:
    results = [check_kafka(), check_postgres()]
    print()
    for r in results:
        mark = "✅" if r["ok"] else "❌"
        print(f"{mark} {r['service']}: {r['detail']}")
    print()
    ok_all = all(r["ok"] for r in results)
    if not ok_all:
        print("Fix the failing services, or leave them unconfigured — the pipeline degrades gracefully.")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
