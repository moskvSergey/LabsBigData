"""Общие утилиты для автоматических проверок ЛР-5."""

import json
import os
import time
import uuid

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
REQUIRED_LATENCY_MS = float(os.environ.get("REQUIRED_LATENCY_MS", "45"))
WORK_DIR = os.environ.get("WORK_DIR", "/opt/work")
METRICS_JSON = os.path.join(WORK_DIR, "metrics.json")

TOPIC_EVENTS = "rating_events"
TOPIC_PREDICTIONS = "predictions"
TOPIC_DLQ_LATE = "dlq_late"
TOPIC_DLQ_ERRORS = "dlq_errors"


def load_metrics():
    """Метрики, посчитанные сервисом monitor по живому потоку."""
    with open(METRICS_JSON, encoding="utf-8") as f:
        return json.load(f)


def make_consumer(topics, group=None, offset="latest"):
    from kafka import KafkaConsumer

    return KafkaConsumer(
        *topics,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset=offset,
        enable_auto_commit=False,
        group_id=group or f"lab5-test-{uuid.uuid4().hex[:8]}",
        consumer_timeout_ms=2000,
    )


def make_producer():
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks=1,
        linger_ms=0,
    )


def send_events(producer, n, late=False, late_delay_s=360.0, user_id=1, movie_id=1):
    """Отправляет n событий и возвращает {event_id: sent_ts}."""
    sent = {}
    now = time.time() * 1000.0
    for i in range(n):
        eid = uuid.uuid4().hex
        event_ms = now - (late_delay_s * 1000.0 if late else 0.0) + i
        sent[eid] = now
        producer.send(TOPIC_EVENTS, json.dumps({
            "event_id": eid,
            "user_id": user_id + (i % 50),
            "movie_id": movie_id + (i % 20),
            "rating": 4.0,
            "event_time": int(event_ms),
            "sent_ts": now,
            "is_late": bool(late),
        }).encode("utf-8"))
    producer.flush()
    return sent


def measure_latencies(n=200, timeout_s=60, user_id=1, movie_id=1):
    """Живой замер end-to-end задержки: отправка -> появление предикта."""
    import numpy as np

    consumer = make_consumer([TOPIC_PREDICTIONS], offset="latest")
    producer = make_producer()
    # прогреваем consumer, чтобы получить назначение партиций
    consumer.poll(timeout_ms=500)

    sent = send_events(producer, n, user_id=user_id, movie_id=movie_id)
    lat = []
    deadline = time.time() + timeout_s
    while len(lat) < n and time.time() < deadline:
        for _tp, records in consumer.poll(timeout_ms=500).items():
            for rec in records:
                try:
                    p = json.loads(rec.value.decode("utf-8", "ignore"))
                except Exception:  # noqa: BLE001
                    continue
                eid = p.get("event_id")
                if eid in sent:
                    lat.append(time.time() * 1000.0 - sent[eid])
                    del sent[eid]
    consumer.close()
    producer.close()
    return np.array(lat) if lat else np.array([])


def count_late_records(timeout_s=45):
    """Считает late-arriving события: из predictions (is_late) и из dlq_late."""
    consumer = make_consumer([TOPIC_PREDICTIONS, TOPIC_DLQ_LATE], offset="latest")
    consumer.poll(timeout_ms=500)
    n_pred_late = 0
    n_dlq_late = 0
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for tp, records in consumer.poll(timeout_ms=500).items():
            for rec in records:
                if tp.topic == TOPIC_DLQ_LATE:
                    n_dlq_late += 1
                    continue
                try:
                    p = json.loads(rec.value.decode("utf-8", "ignore"))
                except Exception:  # noqa: BLE001
                    continue
                if p.get("is_late"):
                    n_pred_late += 1
    consumer.close()
    return n_pred_late, n_dlq_late
