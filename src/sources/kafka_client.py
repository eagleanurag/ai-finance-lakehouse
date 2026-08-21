from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from config import settings


class EventBroker:
    def __init__(self, topic: str | None = None) -> None:
        self.topic = topic or settings.KAFKA_TOPIC
        self.mode = settings.KAFKA_MODE
        self._outbox = settings.OUTBOX_DIR / f"{self.topic.replace('.', '_')}.jsonl"

    @property
    def description(self) -> str:
        if self.mode == "upstash_rest":
            return f"Upstash Kafka REST -> {self.topic}"
        return f"Local file broker -> {self._outbox.name}"

    def produce(self, events: list[dict[str, Any]]) -> int:
        if not events:
            return 0
        if self.mode == "upstash_rest":
            sent = 0
            auth = (settings.UPSTASH_KAFKA_REST_USERNAME, settings.UPSTASH_KAFKA_REST_PASSWORD)
            with requests.Session() as session:
                for event in events:
                    resp = session.post(
                        f"{settings.UPSTASH_KAFKA_REST_URL}/produce/{self.topic}",
                        data=json.dumps(event),
                        auth=auth,
                        timeout=30,
                    )
                    resp.raise_for_status()
                    sent += 1
            return sent
        settings.OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
        with open(self._outbox, "a", encoding="utf-8") as fh:
            for event in events:
                fh.write(json.dumps(event) + "\n")
        return len(events)

    def consume_all(self, max_messages: int = 20000) -> list[dict[str, Any]]:
        if self.mode == "upstash_rest":
            return self._consume_upstash(max_messages)
        return self._consume_file(max_messages)

    def _consume_upstash(self, max_messages: int) -> list[dict[str, Any]]:
        auth = (settings.UPSTASH_KAFKA_REST_USERNAME, settings.UPSTASH_KAFKA_REST_PASSWORD)
        events: list[dict[str, Any]] = []
        url = f"{settings.UPSTASH_KAFKA_REST_URL}/consume/{self.topic}"
        params = {"topic_partition": 0}
        while len(events) < max_messages:
            batch_size = min(1000, max_messages - len(events))
            params["message_count"] = batch_size
            resp = requests.get(url, params=params, auth=auth, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            if isinstance(payload, dict):
                payload = [payload]
            if not payload:
                break
            for item in payload:
                value = item.get("value", "")
                try:
                    decoded = base64.b64decode(value).decode("utf-8")
                except Exception:
                    decoded = str(value)
                events.append(json.loads(decoded))
            if len(payload) < batch_size:
                break
        return events

    def _consume_file(self, max_messages: int) -> list[dict[str, Any]]:
        if not self._outbox.exists():
            return []
        with open(self._outbox, "r", encoding="utf-8") as fh:
            lines = [line for line in fh.read().splitlines() if line.strip()]
        taken = lines[:max_messages]
        remaining = lines[len(taken):]
        tmp = Path(str(self._outbox) + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write("\n".join(remaining) + ("\n" if remaining else ""))
        os.replace(tmp, self._outbox)
        return [json.loads(line) for line in taken]


def partition_path(base: Path, event_date: str) -> Path:
    dt = datetime.now(timezone.utc)
    stamp = dt.strftime("%Y%m%dT%H%M%SZ")
    folder = base / f"dt={event_date}" / f"source={settings.KAFKA_MODE}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"part-{stamp}.jsonl"
