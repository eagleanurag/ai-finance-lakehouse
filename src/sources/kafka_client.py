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
        if self.mode == "aiven_ssl":
            return f"Aiven Apache Kafka (SSL) -> {self.topic}"
        if self.mode == "upstash_rest":
            return f"Upstash Kafka REST -> {self.topic}"
        return f"Local file broker -> {self._outbox.name}"

    def produce(self, events: list[dict[str, Any]]) -> int:
        if not events:
            return 0
        if self.mode == "aiven_ssl":
            return self._produce_native(events)
        if self.mode == "upstash_rest":
            return self._produce_upstash(events)
        return self._produce_file(events)

    def consume_all(self, max_messages: int = 20000) -> list[dict[str, Any]]:
        if self.mode == "aiven_ssl":
            return self._consume_native(max_messages)
        if self.mode == "upstash_rest":
            return self._consume_upstash(max_messages)
        return self._consume_file(max_messages)

    def _kafka_security_config(self) -> dict[str, str]:
        import logging

        log = logging.getLogger(__name__)
        conf = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "security.protocol": settings.KAFKA_SECURITY_PROTOCOL,
            "sasl.mechanisms": settings.KAFKA_SASL_MECHANISM,
            "sasl.username": settings.KAFKA_USERNAME,
            "sasl.password": settings.KAFKA_PASSWORD,
        }
        ca_path = self._ensure_ca()
        if ca_path:
            conf["ssl.ca.location"] = ca_path
        else:
            log.warning(
                "Aiven CA pem not found at %s - SSL verification will fail until it is placed there",
                self._ca_path(),
            )
        cert = settings.PROJECT_ROOT / "data" / "secrets" / "service.cert"
        key = settings.PROJECT_ROOT / "data" / "secrets" / "service.key"
        if cert.exists() and key.exists():
            conf["security.protocol"] = "SSL"
            conf["ssl.certificate.location"] = str(cert)
            conf["ssl.key.location"] = str(key)
        return conf

    @staticmethod
    def _ca_path() -> str:
        return str(settings.PROJECT_ROOT / "data" / "secrets" / "aiven-ca.pem")

    def _ensure_ca(self) -> str | None:
        """Aiven brokers use Aiven's own CA; librdkafka needs an explicit PEM."""
        ca_path = Path(self._ca_path())
        try:
            if ca_path.exists() and ca_path.stat().st_size > 500:
                return str(ca_path)
        except Exception:
            pass
        return None

    def _produce_native(self, events: list[dict[str, Any]]) -> int:
        from confluent_kafka import Producer

        producer = Producer({**self._kafka_security_config(), "acks": "all", "retries": 3})
        for event in events:
            while True:
                try:
                    producer.produce(self.topic, value=json.dumps(event, default=str).encode("utf-8"))
                    break
                except BufferError:
                    producer.poll(0.5)
        producer.flush(60)
        return len(events)

    def _consume_native(self, max_messages: int) -> list[dict[str, Any]]:
        import time
        import uuid

        from confluent_kafka import Consumer

        group = f"lakehouse-drain-{uuid.uuid4().hex[:8]}"
        consumer = Consumer(
            {
                **self._kafka_security_config(),
                "group.id": group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([self.topic])
        events: list[dict[str, Any]] = []
        idle_deadline = time.time() + 20
        try:
            while len(events) < max_messages and time.time() < idle_deadline:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                events.append(json.loads(msg.value().decode("utf-8")))
                idle_deadline = time.time() + 3
            if events:
                consumer.commit(asynchronous=False)
        finally:
            consumer.close()
        return events

    def _produce_upstash(self, events: list[dict[str, Any]]) -> int:
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

    def _produce_file(self, events: list[dict[str, Any]]) -> int:
        settings.OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
        with open(self._outbox, "a", encoding="utf-8") as fh:
            for event in events:
                fh.write(json.dumps(event) + "\n")
        return len(events)

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
