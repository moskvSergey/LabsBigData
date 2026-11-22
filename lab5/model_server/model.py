"""
ML Model Server: online-инференс рекомендательной модели (ONNX Runtime) по HTTP.

Отдельный сервис нужен для:
  • версионирования модели (одна точка загрузки артефакта recommender.onnx:v1);
  • сравнения «embedded ONNX в Flink» vs «HTTP-запрос в model server»
    (см. раздел анализа узких мест: HTTP добавляет ~0.5-2 мс на событие);
  • health-check и метрик Prometheus.

Эндпоинты:
    POST /predict   {"user_avg_rating":..., ...}  -> {"score":..., "will_like":...}
    GET  /health
    GET  /metrics   (Prometheus)
"""

import json
import os
import time

import numpy as np
import onnxruntime as ort
from fastapi import FastAPI
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

MODEL_PATH = os.environ.get("MODEL_PATH", "/opt/models/recommender.onnx")
VERSION_FILE = os.environ.get("VERSION_FILE", "/opt/models/model_version.json")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "recommender.onnx:v1")

app = FastAPI(title="Lab5 Model Server", version="1.0")

REQS = Counter("model_server_requests_total", "Всего запросов инференса", ["status"])
LAT = Histogram(
    "model_server_inference_seconds", "Время инференса",
    buckets=(0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05),
)

FEATURES = [
    "user_avg_rating", "user_rating_count", "user_std_rating",
    "movie_avg_rating", "movie_rating_count", "movie_year",
    "hour", "day_of_week",
]
THRESHOLD = 0.372844

_so = ort.SessionOptions()
_so.intra_op_num_threads = 1
_so.inter_op_num_threads = 1
_session = ort.InferenceSession(MODEL_PATH, sess_options=_so, providers=["CPUExecutionProvider"])
_names = [o.name for o in _session.get_outputs()]
_prob_idx = _names.index("probabilities") if "probabilities" in _names else 0

if os.path.exists(VERSION_FILE):
    with open(VERSION_FILE, encoding="utf-8") as f:
        meta = json.load(f)
    MODEL_VERSION = meta.get("model_version", MODEL_VERSION)
    THRESHOLD = float(meta.get("decision_threshold", THRESHOLD))


class Features(BaseModel):
    user_avg_rating: float = 3.5
    user_rating_count: float = 0.0
    user_std_rating: float = 0.0
    movie_avg_rating: float = 3.5
    movie_rating_count: float = 0.0
    movie_year: float = 1995.0
    hour: float = 12.0
    day_of_week: float = 0.0


@app.get("/health")
def health():
    return {"status": "ok", "model_version": MODEL_VERSION, "features": FEATURES}


@app.post("/predict")
def predict(f: Features):
    t0 = time.perf_counter()
    vec = np.array([[getattr(f, name) for name in FEATURES]], dtype=np.float32)
    score = float(np.asarray(_session.run(None, {"input": vec})[_prob_idx])[:, 1][0])
    LAT.observe(time.perf_counter() - t0)
    REQS.labels(status="ok").inc()
    return {
        "score": round(score, 6),
        "will_like": bool(score >= THRESHOLD),
        "model_version": MODEL_VERSION,
    }


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
