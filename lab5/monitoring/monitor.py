"""
Мониторинг потокового пайплайна ЛР-5.

Что делает:
  1. Читает топики predictions / dlq_late / dlq_errors;
  2. Считает end-to-end задержку (отправка в топик -> появление предикта) и
     задержку инференса (отправка -> инференс в Flink);
  3. Считает PSI (Population Stability Index) по фиче user_avg_rating
     относительно offline-распределения (снапшот фиче-стора) — детекция дрейфа;
  4. Читает длительность последнего checkpoint из метрик Flink;
  5. Отдаёт всё в Prometheus (/metrics на :8001) и печатает фиксированный
     блок ## METRICS ## (требование отчёта), а также пишет work/metrics.json.

Запуск: python monitor.py
"""

import json
import os
import re
import threading
import time
import urllib.request
from collections import defaultdict

import numpy as np
from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
REQUIRED_LATENCY_MS = float(os.environ.get("REQUIRED_LATENCY_MS", "45"))
RUN_SECONDS = float(os.environ.get("RUN_SECONDS", "90"))
WORK_DIR = os.environ.get("WORK_DIR", "/opt/work")
FEATURE_JSON = os.environ.get("FEATURE_JSON", os.path.join(WORK_DIR, "feature_store.json"))
FLINK_METRICS_URLS = os.environ.get(
    "FLINK_METRICS_URLS", "http://flink-jobmanager:9250/metrics,http://flink-taskmanager:9251/metrics"
).split(",")
DRIFT_THRESHOLD = 0.2

TOPIC_PREDICTIONS = "predictions"
TOPIC_DLQ_LATE = "dlq_late"
TOPIC_DLQ_ERRORS = "dlq_errors"

# --- Prometheus-метрики ---
G_EVENTS = Gauge("lab5_predictions_total", "Число полученных предиктов")
G_LATE = Gauge("lab5_late_events_total", "Число late-arriving событий")
G_ERRORS = Gauge("lab5_errors_total", "Число ошибок (DLQ)")
G_P50 = Gauge("lab5_latency_p50_ms", "End-to-end задержка p50, мс")
G_P95 = Gauge("lab5_latency_p95_ms", "End-to-end задержка p95, мс")
G_P99 = Gauge("lab5_latency_p99_ms", "End-to-end задержка p99, мс")
G_REQ = Gauge("lab5_required_latency_ms", "Уникальное требование к задержке, мс")
G_SLA = Gauge("lab5_sla_ok", "1 если p99 < требования, иначе 0")
G_EPS = Gauge("lab5_throughput_eps", "Пропускная способность, событий/с")
G_DRIFT = Gauge("lab5_data_drift_psi", "PSI по фиче user_avg_rating")
G_CKPT = Gauge("lab5_checkpoint_duration_sec", "Длительность последнего checkpoint, с")
G_INFER_P99 = Gauge("lab5_inference_p99_ms", "Задержка инференса p99, мс")
H_LAT = Histogram("lab5_e2e_latency_ms", "End-to-end задержка, мс",
                  buckets=(1, 5, 10, 20, 30, 45, 60, 100, 250, 500, 1000, 5000))
C_SEEN = Counter("lab5_prediction_ids_total", "Уникальные event_id (идемпотентность)",
                 ["kind"])

LAT = []
PIPELINE_LAT = []
INFER_MS = []
SEEN = set()
DUPLICATES = 0
LATE_EVENTS = 0
ERRORS = 0
START = time.time()
FIRST_RECV = [None]
LAST_RECV = [None]
U_AVG_ONLINE = []
LOCK = threading.Lock()


def load_reference_user_avg():
    """Offline-распределение user_avg_rating (референс для PSI)."""
    if not os.path.exists(FEATURE_JSON):
        return None
    with open(FEATURE_JSON, encoding="utf-8") as f:
        data = json.load(f)
    vals = np.array([u["user_avg_rating"] for u in data["users"]], dtype=float)
    return vals


def psi(reference: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    """Population Stability Index. > 0.2 — значимый дрейф."""
    if reference is None or len(current) < 50:
        return 0.0
    edges = np.quantile(reference, np.linspace(0, 1, bins + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    edges = np.unique(edges)
    ref_hist, _ = np.histogram(reference, bins=edges)
    cur_hist, _ = np.histogram(np.asarray(current), bins=edges)
    ref_pct = np.clip(ref_hist / max(ref_hist.sum(), 1), 1e-6, None)
    cur_pct = np.clip(cur_hist / max(cur_hist.sum(), 1), 1e-6, None)
    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def read_checkpoint_duration():
    """Последняя длительность checkpoint из метрик Flink (Prometheus-формат)."""
    # имя метрики может сопровождаться {labels}, поэтому допускаем обе формы
    pattern = re.compile(
        r"flink_\w*lastCheckpointDuration\w*(?:\{[^}]*\})?\s+([0-9.eE+-]+)"
    )
    for url in FLINK_METRICS_URLS:
        try:
            with urllib.request.urlopen(url.strip(), timeout=3) as resp:
                body = resp.read().decode("utf-8", "ignore")
            m = pattern.search(body)
            if m:
                # Flink отдаёт длительность checkpoint в миллисекундах
                return float(m.group(1)) / 1000.0
        except Exception:  # noqa: BLE001
            continue
    return None


def consume(topics, stop_event):
    global DUPLICATES, LATE_EVENTS, ERRORS
    consumer = None
    for _ in range(30):
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=BOOTSTRAP,
                # latest: замеряем только события, отправленные ПОСЛЕ старта монитора,
                # иначе в перцентили попадут старые сообщения из топика.
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id=f"lab5-monitor-{int(time.time())}",
                consumer_timeout_ms=3000,
                # быстрый опрос: иначе consumer добавляет до 500 мс к замеру
                fetch_max_wait_ms=5,
                fetch_min_bytes=1,
            )
            break
        except Exception:  # noqa: BLE001
            time.sleep(2)
    if consumer is None:
        print("[monitor] Kafka недоступна", flush=True)
        return

    while not stop_event.is_set():
        try:
            batches = consumer.poll(timeout_ms=500)
        except Exception:  # noqa: BLE001
            time.sleep(1)
            continue
        for tp, records in batches.items():
            topic = tp.topic
            for rec in records:
                try:
                    payload = json.loads(rec.value.decode("utf-8", "ignore"))
                except Exception:  # noqa: BLE001
                    continue
                if topic == TOPIC_PREDICTIONS:
                    with LOCK:
                        eid = payload.get("event_id")
                        if eid in SEEN:
                            DUPLICATES += 1
                            C_SEEN.labels(kind="duplicate").inc()
                        else:
                            SEEN.add(eid)
                            C_SEEN.labels(kind="unique").inc()
                        recv = time.time() * 1000.0
                        if FIRST_RECV[0] is None:
                            FIRST_RECV[0] = recv
                        LAST_RECV[0] = recv
                        e2e = recv - float(payload["sent_ts"])
                        LAT.append(e2e)
                        H_LAT.observe(e2e)
                        # задержка конвейера: отправка -> штамп инференса во Flink
                        if "infer_ts" in payload:
                            PIPELINE_LAT.append(float(payload["infer_ts"]) - float(payload["sent_ts"]))
                        # собственно время инференса модели (замер внутри Flink)
                        if "infer_ms" in payload:
                            INFER_MS.append(float(payload["infer_ms"]))
                        if payload.get("is_late"):
                            LATE_EVENTS += 1
                        ua = payload.get("u_avg")
                        if ua is not None:
                            U_AVG_ONLINE.append(float(ua))
                elif topic == TOPIC_DLQ_LATE:
                    with LOCK:
                        LATE_EVENTS += 1
                elif topic == TOPIC_DLQ_ERRORS:
                    with LOCK:
                        ERRORS += 1
    try:
        consumer.close()
    except Exception:  # noqa: BLE001
        pass


def summarize():
    with LOCK:
        lat = np.array(LAT) if LAT else np.array([0.0])
        pipe = np.array(PIPELINE_LAT) if PIPELINE_LAT else np.array([0.0])
        inf = np.array(INFER_MS) if INFER_MS else np.array([0.0])
        n = len(LAT)
        # пропускную способность считаем по «активному» окну потока,
        # а не по всему времени работы монитора (иначе простоит в знаменателе)
        if FIRST_RECV[0] and LAST_RECV[0] and LAST_RECV[0] > FIRST_RECV[0]:
            active_s = (LAST_RECV[0] - FIRST_RECV[0]) / 1000.0
        else:
            active_s = max(time.time() - START, 1e-6)
        drift = psi(REFERENCE, np.array(U_AVG_ONLINE))
        return {
            "predictions": n,
            "unique_event_ids": len(SEEN),
            "duplicates": DUPLICATES,
            "late_events": LATE_EVENTS,
            "errors": ERRORS,
            "p50": float(np.percentile(lat, 50)),
            "p95": float(np.percentile(lat, 95)),
            "p99": float(np.percentile(lat, 99)),
            "pipeline_p50": float(np.percentile(pipe, 50)),
            "pipeline_p99": float(np.percentile(pipe, 99)),
            "infer_p50": float(np.percentile(inf, 50)),
            "infer_p99": float(np.percentile(inf, 99)),
            "throughput": n / active_s,
            "drift_psi": drift,
            "error_rate": ERRORS / max(n + ERRORS, 1),
        }


REFERENCE = None


def main():
    global REFERENCE
    REFERENCE = load_reference_user_avg()
    print(f"[monitor] референс для PSI: "
          f"{'загружен, n=' + str(len(REFERENCE)) if REFERENCE is not None else 'нет'}",
          flush=True)

    start_http_server(8001)
    print("[monitor] Prometheus /metrics на :8001", flush=True)

    stop_event = threading.Event()
    t = threading.Thread(target=consume, args=([TOPIC_PREDICTIONS, TOPIC_DLQ_LATE, TOPIC_DLQ_ERRORS], stop_event),
                         daemon=True)
    t.start()

    last_print = 0.0
    while True:
        time.sleep(2)
        s = summarize()
        G_EVENTS.set(s["predictions"])
        G_LATE.set(s["late_events"])
        G_ERRORS.set(s["errors"])
        G_P50.set(s["p50"])
        G_P95.set(s["p95"])
        G_P99.set(s["p99"])
        G_INFER_P99.set(s["infer_p99"])
        G_REQ.set(REQUIRED_LATENCY_MS)
        G_SLA.set(1.0 if s["p99"] < REQUIRED_LATENCY_MS else 0.0)
        G_EPS.set(s["throughput"])
        G_DRIFT.set(s["drift_psi"])
        ck = read_checkpoint_duration()
        if ck is not None:
            G_CKPT.set(ck)

        if time.time() - last_print > 15:
            last_print = time.time()
            print(f"[monitor] предиктов={s['predictions']} p50={s['p50']:.1f}мс "
                  f"p95={s['p95']:.1f}мс p99={s['p99']:.1f}мс late={s['late_events']} "
                  f"errors={s['errors']} psi={s['drift_psi']:.3f}", flush=True)

        if time.time() - START > RUN_SECONDS and s["predictions"] > 0:
            break

    stop_event.set()
    time.sleep(1)
    s = summarize()
    ck = read_checkpoint_duration() or 0.0
    sla_ok = s["p99"] < REQUIRED_LATENCY_MS

    os.makedirs(WORK_DIR, exist_ok=True)
    result = {
        "required_latency_ms": REQUIRED_LATENCY_MS,
        "measured_p50_latency_ms": round(s["p50"], 3),
        "measured_p95_latency_ms": round(s["p95"], 3),
        "measured_p99_latency_ms": round(s["p99"], 3),
        "inference_p50_ms": round(s["infer_p50"], 3),
        "inference_p99_ms": round(s["infer_p99"], 3),
        "pipeline_p50_ms": round(s["pipeline_p50"], 3),
        "pipeline_p99_ms": round(s["pipeline_p99"], 3),
        "sla_ok": sla_ok,
        "predictions": s["predictions"],
        "unique_event_ids": s["unique_event_ids"],
        "duplicates": s["duplicates"],
        "late_events": s["late_events"],
        "late_data_handled": s["late_events"] > 0,
        "errors": s["errors"],
        "error_rate": round(s["error_rate"], 6),
        "throughput_eps": round(s["throughput"], 2),
        "checkpoint_duration_sec": round(float(ck), 4),
        "data_drift_psi": round(s["drift_psi"], 4),
        "drift_detected": s["drift_psi"] > DRIFT_THRESHOLD,
        "ts": time.time(),
    }
    with open(os.path.join(WORK_DIR, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("## METRICS ##", flush=True)
    print(f"REQUIRED_LATENCY={REQUIRED_LATENCY_MS:g}", flush=True)
    print(f"MEASURED_P50_LATENCY={s['p50']:.2f}", flush=True)
    print(f"MEASURED_P95_LATENCY={s['p95']:.2f}", flush=True)
    print(f"MEASURED_P99_LATENCY={s['p99']:.2f}", flush=True)
    print(f"INFERENCE_P99_LATENCY={s['infer_p99']:.2f}", flush=True)
    print(f"PIPELINE_P99_LATENCY={s['pipeline_p99']:.2f}", flush=True)
    print(f"SLA_OK={sla_ok}", flush=True)
    print(f"LATE_DATA_HANDLED={s['late_events'] > 0}", flush=True)
    print(f"LATE_EVENTS={s['late_events']}", flush=True)
    print(f"PREDICTIONS={s['predictions']}", flush=True)
    print(f"DUPLICATES={s['duplicates']}", flush=True)
    print(f"ERRORS={s['errors']}", flush=True)
    print(f"ERROR_RATE={s['error_rate']:.4f}", flush=True)
    print(f"THROUGHPUT_EPS={s['throughput']:.1f}", flush=True)
    print(f"CHECKPOINT_DURATION_SEC={float(ck):.2f}", flush=True)
    print(f"DATA_DRIFT_SCORE={s['drift_psi']:.4f}", flush=True)

    # Остаёмся жить, чтобы Prometheus мог скрейпить метрики
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
