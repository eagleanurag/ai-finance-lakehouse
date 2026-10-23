"""Always-on streaming loop behind the app's live view.

One background thread per app process produces POS/ERP events every few seconds, consumes them back
from the broker, lands them in bronze (picked up by the next notebook run) and keeps the recent ones
in memory for the live dashboard. Aiven Kafka is used whenever it is reachable; otherwise an embedded
in-process broker keeps the stream flowing and Kafka is retried periodically.
"""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np

from config import settings
from src.pipelines import bronze
from src.sources.kafka_client import EventBroker

log = logging.getLogger(__name__)

KAFKA_RETRY_SECONDS = 300
RETENTION_DAYS = 3


class _KafkaTransport:
    name = "Aiven Kafka"

    def __init__(self, broker: EventBroker) -> None:
        from confluent_kafka import Consumer, Producer

        conf = {**broker._kafka_security_config(), "log_level": 0}
        self.topic = broker.topic
        self.producer = Producer({**conf, "acks": "all", "linger.ms": 50})
        self.consumer = Consumer({**conf, "group.id": "lakehouse-live-view", "auto.offset.reset": "latest",
                                  "enable.auto.commit": True})
        self.consumer.subscribe([self.topic])

    def send(self, events: list[dict[str, Any]]) -> None:
        for event in events:
            self.producer.produce(self.topic, value=json.dumps(event, default=str).encode("utf-8"))
        self.producer.flush(10)

    def receive(self, wait: float) -> list[dict[str, Any]]:
        out, deadline = [], time.time() + wait
        while time.time() < deadline:
            msg = self.consumer.poll(0.2)
            if msg is None or msg.error():
                continue
            out.append(json.loads(msg.value().decode("utf-8")))
        return out

    def close(self) -> None:
        try:
            self.consumer.close()
        except Exception:
            pass


class _EmbeddedTransport:
    name = "Embedded broker"

    def __init__(self) -> None:
        self.q: queue.Queue = queue.Queue()

    def send(self, events: list[dict[str, Any]]) -> None:
        for event in events:
            self.q.put(json.dumps(event, default=str))

    def receive(self, wait: float) -> list[dict[str, Any]]:
        out = []
        try:
            out.append(json.loads(self.q.get(timeout=wait)))
            while True:
                out.append(json.loads(self.q.get_nowait()))
        except queue.Empty:
            pass
        return out

    def close(self) -> None:
        pass


class LiveStream:
    def __init__(self, interval: float | None = None) -> None:
        self.interval = interval or settings.LIVE_STREAM_INTERVAL
        self.recent: deque[dict[str, Any]] = deque(maxlen=2000)
        self.lock = threading.Lock()
        self.stats: dict[str, Any] = {"produced": 0, "consumed": 0, "landed": 0, "errors": 0,
                                      "started_at": datetime.now(timezone.utc), "last_event_at": None,
                                      "kafka_status": "checking...", "transport": "starting"}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._transport: _KafkaTransport | _EmbeddedTransport | None = None
        self._last_kafka_try = 0.0

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> "LiveStream":
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._run, name="live-stream", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()

    def _connect(self) -> None:
        self._last_kafka_try = time.time()
        broker = EventBroker()
        ok, reason = broker.reachable()
        self.stats["kafka_status"] = reason
        if ok:
            try:
                transport = _KafkaTransport(broker)
                if self._transport:
                    self._transport.close()
                self._transport = transport
            except Exception as exc:
                self.stats["kafka_status"] = f"client error: {exc}"
        if self._transport is None:
            self._transport = _EmbeddedTransport()
        self.stats["transport"] = self._transport.name
        log.info("Live stream using %s (%s)", self._transport.name, self.stats["kafka_status"])

    def _run(self) -> None:
        from src.pipelines.ingest import make_generator

        gen = make_generator(target_rows=1000)
        rng = np.random.default_rng()
        self._connect()
        while not self._stop.is_set():
            tick = time.time()
            try:
                if isinstance(self._transport, _EmbeddedTransport) and tick - self._last_kafka_try > KAFKA_RETRY_SECONDS:
                    self._connect()
                hour = datetime.now(timezone.utc).hour
                burst = int(rng.poisson(2.5 if 8 <= hour < 20 else 1.2)) + 1  # busier in business hours
                events = gen.live_events(burst, rng)
                sent_at = time.time()
                self._transport.send(events)
                self.stats["produced"] += len(events)
                received = self._transport.receive(wait=min(2.0, self.interval * 0.8))
                if received:
                    self._land(received, sent_at)
            except Exception as exc:  # never let the loop die; fall back to the embedded broker
                self.stats["errors"] += 1
                self.stats["kafka_status"] = f"stream error, falling back: {exc}"
                log.warning("Live stream error: %s", exc)
                if isinstance(self._transport, _KafkaTransport):
                    self._transport.close()
                    self._transport = _EmbeddedTransport()
                    self.stats["transport"] = self._transport.name
            self._stop.wait(max(0.2, self.interval - (time.time() - tick)))

    # ------------------------------------------------------------------ landing
    def _land(self, events: list[dict[str, Any]], sent_at: float) -> None:
        now = datetime.now(timezone.utc)
        latency_ms = round((time.time() - sent_at) * 1000)
        rows = []
        for env in events:
            p = env.get("payload", env)
            rows.append({**p, "received_at": now, "latency_ms": latency_ms,
                         "producer": env.get("headers", {}).get("producer")})
        path = bronze.partition_dir(settings.BRONZE_TXN_DIR, now.date().isoformat(), "kafka_live") / f"part-live-{now:%Y%m%dT%H}.jsonl"
        with open(path, "a", encoding="utf-8") as fh:
            for env in events:
                fh.write(json.dumps(env, default=str) + "\n")
        with self.lock:
            self.recent.extend(rows)
            self.stats["consumed"] += len(events)
            self.stats["landed"] += len(events)
            self.stats["last_event_at"] = now
        if now.minute == 0 and now.second < self.interval:
            self._prune()

    def _prune(self) -> None:
        cutoff = date.today() - timedelta(days=RETENTION_DAYS)
        for folder in settings.BRONZE_TXN_DIR.glob("dt=*/source=kafka_live"):
            try:
                if date.fromisoformat(folder.parent.name[3:]) < cutoff:
                    for f in folder.glob("*.jsonl"):
                        f.unlink()
            except ValueError:
                continue

    # ------------------------------------------------------------------ read side
    def snapshot(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        with self.lock:
            return list(self.recent), dict(self.stats)
