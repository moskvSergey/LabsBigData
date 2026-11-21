"""
ЛР-6, Часть 1. ML Model Server: онлайн-инференс рекомендательной модели.

Минимальный, но настоящий сервер: линейная модель (логистика с весами,
посчитанными офлайн на MovieLens) + FastAPI + метрики Prometheus.
Специально без ONNX/MLflow — они уже были в ЛР-5; здесь задача не про ML,
а про DevOps: образ, лимиты ресурсов, health/readiness, метрики, дрейф.

Эндпоинты:
  GET  /health          — liveness/readiness для Kubernetes
  GET  /metrics         — метрики Prometheus (latency, rps, drift, cost)
  POST /predict         — инференс одного события
  POST /predict/batch   — инференс пачкой
  POST /metrics/ingest  — приём бизнес-метрик (PSI, cost per job) от пайплайна
"""

from __future__ import annotations

import math
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, List

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

MODEL_VERSION = os.environ.get("MODEL_VERSION", "lab6-lr-1.0")
# Веса «обучены» офлайн на MovieLens ml-32m (ЛР-1..ЛР-5), здесь только инференс.
WEIGHTS = {"user_avg": 0.62, "movie_avg": 0.55, "user_count": 0.0007, "rating": 0.41}
BIAS = -3.10
LATENCY_WINDOW = 2000
DRIFT_BINS = 10
# Эталонное распределение рейтингов (offline, ml-32m): доли по 10 бинам 0.5..5.0
REFERENCE_HIST = [0.02, 0.03, 0.06, 0.10, 0.14, 0.19, 0.21, 0.16, 0.07, 0.02]


def _band(value: float) -> int:
    ratio = (value - 0.5) / (5.0 - 0.5)
    return min(DRIFT_BINS - 1, max(0, int(ratio * DRIFT_BINS)))


app = FastAPI(title="ЛР-6 Model Server", version=MODEL_VERSION)


class RatingEvent(BaseModel):
    event_id: str = Field(default="evt-0")
    user_id: int = 0
    movie_id: int = 0
    rating: float = 0.0
    user_avg_rating: float = 3.5
    movie_avg_rating: float = 3.5
    user_rating_count: int = 1
    event_time: str | None = None


class IngestPayload(BaseModel):
    """Метрики, которые считает пайплайн (не сам сервер)."""

    data_drift_psi: float | None = None
    cost_per_job: float | None = None
    cost_savings_pct: float | None = None
    schema_compliance_ratio: float | None = None
    dlq_records_total: int | None = None


class ModelServer:
    """Состояние сервера: метрики и гистограмма для расчёта PSI."""

    def __init__(self) -> None:
        self.latencies: Deque[float] = deque(maxlen=LATENCY_WINDOW)
        self.counters = {"requests": 0, "errors": 0, "predictions": 0, "dlq": 0}
        self.hist = [0] * DRIFT_BINS
        self.started = time.time()
        self.ingested: dict = {}

    # --- latency ---
    def observe_latency(self, ms: float) -> None:
        self.latencies.append(ms)

    def percentile(self, q: float) -> float:
        if not self.latencies:
            return 0.0
        ordered = sorted(self.latencies)
        return ordered[min(len(ordered) - 1, int(round(q / 100.0 * (len(ordered) - 1))))]

    # --- drift ---
    def observe_rating(self, rating: float) -> None:
        self.hist[_band(rating)] += 1

    def psi(self) -> float:
        total = sum(self.hist)
        if total < 100:
            return 0.0
        current = [max(c / total, 1e-6) for c in self.hist]
        return sum((c - r) * math.log(c / r) for r, c in zip(REFERENCE_HIST, current))

    def schema_compliance(self) -> float:
        total = self.counters["requests"]
        return 1.0 if total == 0 else max(0.0, 1.0 - self.counters["errors"] / total)

    def score(self, e: RatingEvent) -> float:
        features = [e.user_avg_rating, e.movie_avg_rating,
                    math.log1p(max(e.user_rating_count, 0)), e.rating]
        z = BIAS + sum(w * x for w, x in zip(WEIGHTS.values(), features))
        return 1.0 / (1.0 + math.exp(-z))

    # --- Prometheus text format ---
    def render(self) -> str:
        cost_cfg = _cost_metrics()
        drift = self.ingested.get("data_drift_psi")
        if drift is None:
            drift = self.psi()
        cost_per_job = self.ingested.get("cost_per_job", cost_cfg.get("cost_per_job", 0.0))
        savings = self.ingested.get("cost_savings_pct", cost_cfg.get("cost_savings_pct", 0.0))
        compliance = self.ingested.get("schema_compliance_ratio", self.schema_compliance())
        dlq = self.ingested.get("dlq_records_total", self.counters["dlq"])
        uptime = time.time() - self.started
        lines = [
            "# HELP lab6_inference_latency_ms_ms Latency of single inference, ms",
            "# TYPE lab6_inference_latency_ms_ms gauge",
            f'lab6_inference_latency_ms_ms{{quantile="0.5"}} {self.percentile(50):.4f}',
            f'lab6_inference_latency_ms_ms{{quantile="0.95"}} {self.percentile(95):.4f}',
            f'lab6_inference_latency_ms_ms{{quantile="0.99"}} {self.percentile(99):.4f}',
            "# HELP lab6_predictions_total Total predictions",
            "# TYPE lab6_predictions_total counter",
            f"lab6_predictions_total {self.counters['predictions']}",
            "# HELP lab6_requests_total Total requests",
            "# TYPE lab6_requests_total counter",
            f"lab6_requests_total {self.counters['requests']}",
            "# HELP lab6_errors_total Total inference errors",
            "# TYPE lab6_errors_total counter",
            f"lab6_errors_total {self.counters['errors']}",
            "# HELP lab6_uptime_seconds Uptime",
            "# TYPE lab6_uptime_seconds gauge",
            f"lab6_uptime_seconds {uptime:.1f}",
            "# HELP lab6_data_drift_psi Population Stability Index for ratings",
            "# TYPE lab6_data_drift_psi gauge",
            f"lab6_data_drift_psi {drift:.4f}",
            "# HELP lab6_schema_compliance_ratio Fraction of valid records",
            "# TYPE lab6_schema_compliance_ratio gauge",
            f"lab6_schema_compliance_ratio {compliance:.4f}",
            "# HELP lab6_dlq_records_total Records routed to DLQ",
            "# TYPE lab6_dlq_records_total counter",
            f"lab6_dlq_records_total {dlq}",
            "# HELP lab6_cost_per_job_usd Optimized cost per job",
            "# TYPE lab6_cost_per_job_usd gauge",
            f"lab6_cost_per_job_usd {cost_per_job:.4f}",
            "# HELP lab6_cost_savings_pct Cost savings vs baseline, percent",
            "# TYPE lab6_cost_savings_pct gauge",
            f"lab6_cost_savings_pct {savings:.2f}",
            "# HELP lab6_model_version_info Model version",
            "# TYPE lab6_model_version_info gauge",
            f'lab6_model_version_info{{version="{MODEL_VERSION}"}} 1',
        ]
        return "\n".join(lines) + "\n"


def _cost_metrics() -> dict:
    """Подтягиваем cost-цифры из общей модели, если она доступна рядом."""
    import json
    here = os.path.dirname(os.path.abspath(__file__))
    for path in (
        os.environ.get("COST_METRICS_JSON", ""),
        os.path.join(here, "cost_metrics.json"),
        os.path.join(here, "..", "work", "cost_model.json"),
    ):
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                continue
    return {}


SERVER = ModelServer()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_version": MODEL_VERSION,
        "uptime_sec": round(time.time() - SERVER.started, 1),
        "predictions": SERVER.counters["predictions"],
    }


@app.post("/predict")
def predict(event: RatingEvent):
    SERVER.counters["requests"] += 1
    t0 = time.perf_counter()
    try:
        score = SERVER.score(event)
        SERVER.counters["predictions"] += 1
        SERVER.observe_rating(event.rating)
    except Exception:
        SERVER.counters["errors"] += 1
        raise
    finally:
        SERVER.observe_latency((time.perf_counter() - t0) * 1000.0)
    return {
        "event_id": event.event_id,
        "score": round(score, 6),
        "recommend": score >= 0.5,
        "model_version": MODEL_VERSION,
        "infer_ts": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/predict/batch")
def predict_batch(events: List[RatingEvent]):
    return [predict(e) for e in events]


@app.post("/metrics/ingest")
def ingest(payload: IngestPayload):
    """Пайплайн отдаёт сюда бизнес-метрики, посчитанные по своим данным."""
    for key, value in payload.model_dump(exclude_none=True).items():
        if key == "dlq_records_total":
            SERVER.counters["dlq"] = int(value)
        else:
            SERVER.ingested[key] = float(value)
    return {"status": "accepted", "metrics": SERVER.ingested}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    """Prometheus text format. Скрейпится Prometheus'ом (Часть 5)."""
    return SERVER.render()
