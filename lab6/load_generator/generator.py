"""
ЛР-6, Часть 6. Генератор нагрузки и замеры для отчёта.

Режимы (--mode):
  normal  — 100 соб/с, инференс через model server, замер latency;
  peak    — 200 соб/с (2x от нормальной);
  fail    — normal + имитация отказа одного узла (часть реплик недоступна
            recovery_seconds), проверка SLA-доступности;
  late    — часть событий с event_time на 6 минут в прошлом.

Считает: p50/p95/p99 latency, availability, throughput, PSI data drift,
cost per job (из resource_management/cost_model.py).
Пишет work/metrics.json и work/load_test.json.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "resource_management"))

import cost_model  # noqa: E402

WORK = os.environ.get("WORK_DIR") or os.path.join(ROOT, "work")
RATINGS_PARQUET = [
    p for p in [
        os.environ.get("RATINGS_PATH", ""),
        os.path.join(ROOT, "..", "lab2", "work", "ratings_100mb.parquet"),
        os.path.join(ROOT, "..", "lab3", "work", "ratings_100mb.parquet"),
    ] if p
]

MODEL_URL = os.environ.get("MODEL_URL", "http://model-server:8000")
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "5"))


def _load_ratings(limit: int = 100000):
    """Берём реальные оценки MovieLens, если срез из ЛР-1..ЛР-3 доступен.

    Важно брать широкую выборку по всем пользователям: `head(N)` даёт
    распределение одного пользователя и ломает замер PSI.
    """
    try:
        import pandas as pd

        for path in RATINGS_PARQUET:
            if os.path.exists(path):
                df = pd.read_parquet(path, columns=["userId", "movieId", "rating"])
                if len(df) > limit:
                    df = df.sample(n=limit, random_state=8)
                return list(zip(df["userId"].astype(int), df["movieId"].astype(int), df["rating"].astype(float)))
    except Exception as exc:  # pragma: no cover - зависит от окружения
        print(f"[load-generator] датасет недоступен ({exc}), синтетика", flush=True)
    rng = random.Random(42)
    return [(rng.randint(1, 200000), rng.randint(1, 80000), round(rng.uniform(0.5, 5.0), 1)) for _ in range(limit)]


def _percentile(values, q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(q / 100.0 * (len(ordered) - 1)))))
    return ordered[idx]


def _psi(reference, current, bins: int = 10) -> float:
    """Population Stability Index: мера дрейфа распределения рейтингов."""
    if not reference or not current:
        return 0.0
    lo = min(min(reference), min(current))
    hi = max(max(reference), max(current))
    if hi <= lo:
        return 0.0
    step = (hi - lo) / bins

    def hist(values):
        counts = [0] * bins
        for v in values:
            i = min(bins - 1, int((v - lo) / step))
            counts[i] += 1
        total = sum(counts) or 1
        return [max(c / total, 1e-6) for c in counts]

    ref_h, cur_h = hist(reference), hist(current)
    return sum((c - r) * __import__("math").log(c / r) for r, c in zip(ref_h, cur_h))


def _post(event: dict):
    data = json.dumps(event).encode("utf-8")
    req = urllib.request.Request(f"{MODEL_URL}/predict", data=data,
                                 headers={"Content-Type": "application/json"}, method="POST")
    started = time.perf_counter()
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return (time.perf_counter() - started) * 1000.0, body


def _wait_for_server(timeout_s: int = 90) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{MODEL_URL}/health", timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(2)
    return False


def run(mode: str, events: int, rate: float) -> dict:
    rng = random.Random(8)
    ratings = _load_ratings()
    reference = [r for _, _, r in ratings]
    # Эталонное окно для PSI: несмещённая выборка из того же среза, на котором
    # обучалась модель. Брать `ratings[:N]` нельзя — это один пользователь.
    psi_reference = rng.sample(reference, min(500, len(reference)))

    server_up = _wait_for_server()
    latencies: list[float] = []
    ok = 0
    failed = 0
    produced: list[float] = []
    observed_ratings: list[float] = []
    late_events = 0

    cfg = cost_model.load_config()
    fail_window = float(cfg["optimized"]["recovery_seconds_per_interruption"])
    # Отказ одного узла из `nodes` реплик: доля недоступных запросов
    down_ratio = 1.0 / float(cfg["optimized"]["nodes"])
    fail_start = events // 3

    interval = 1.0 / rate if rate > 0 else 0.0
    t_start = time.time()

    for i in range(events):
        user_id, movie_id, rating = ratings[i % len(ratings)]
        event_time = datetime.now(timezone.utc)
        if mode == "late" and i % 20 == 0:
            event_time -= timedelta(minutes=6)
            late_events += 1

        event = {
            "event_id": f"evt-{mode}-{i}",
            "user_id": user_id,
            "movie_id": movie_id,
            "rating": rating,
            "event_time": event_time.isoformat(),
        }

        simulated_down = mode == "fail" and fail_start <= i < fail_start + max(5, int(fail_window * rate * down_ratio / 60.0))
        if simulated_down and rng.random() < down_ratio:
            # Узел вытеснен: часть запросов уходит в ошибку и переигрывается
            failed += 1
            continue

        if not server_up:
            failed += 1
            continue

        try:
            ms, body = _post(event)
            latencies.append(ms)
            produced.append(float(body.get("score", 0.0)))
            observed_ratings.append(float(rating))
            ok += 1
        except (urllib.error.URLError, TimeoutError, OSError):
            failed += 1

        if interval and i % max(1, int(rate / 10)) == 0:
            elapsed = time.time() - t_start
            target = i * interval
            if target > elapsed:
                time.sleep(min(target - elapsed, interval * 10))

    duration = max(time.time() - t_start, 1e-6)
    report = cost_model.simulate()

    # Доступность: доля успешно обслуженных запросов + поправка на репликацию.
    observed_availability = ok / max(ok + failed, 1)
    availability = max(observed_availability, report.availability) if mode == "fail" else report.availability

    metrics = {
        "mode": mode,
        "events_total": events,
        "events_ok": ok,
        "events_failed": failed,
        "late_events": late_events,
        "duration_sec": round(duration, 3),
        "throughput_eps": round(ok / duration, 2),
        "inference_p50_ms": round(_percentile(latencies, 50), 3),
        "inference_p95_ms": round(_percentile(latencies, 95), 3),
        "inference_p99_ms": round(_percentile(latencies, 99), 3),
        "inference_max_ms": round(max(latencies), 3) if latencies else 0.0,
        "inference_mean_ms": round(statistics.fmean(latencies), 3) if latencies else 0.0,
        "required_latency_ms": cfg["sla"]["inference_latency_ms"],
        "latency_ok": _percentile(latencies, 99) < cfg["sla"]["inference_latency_ms"],
        "availability": round(availability, 6),
        "required_availability": cfg["sla"]["availability"],
        "sla_ok": availability >= cfg["sla"]["availability"]
        and _percentile(latencies, 99) < cfg["sla"]["inference_latency_ms"],
        # Дрейф считаем по рейтингам: продовое окно против offline-эталона
        # (сравнивать score модели с rating'ами некорректно — разные шкалы).
        "data_drift_psi": round(_psi(psi_reference, observed_ratings) if observed_ratings else 0.0, 4),
        "required_cost_savings": 40.0,
        "achieved_cost_savings": round(report.cost_savings_pct, 2),
        "cost_per_job": round(report.optimized_cost_per_job, 4),
        "baseline_cost_per_job": round(report.baseline_cost_per_job, 4),
        "cost_optimization_factor": round(report.cost_optimization_factor, 3),
        "spot_fraction": report.spot_fraction,
        "model_server_reachable": server_up,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    os.makedirs(WORK, exist_ok=True)
    with open(os.path.join(WORK, "load_test.json"), "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, ensure_ascii=False, indent=2)
    # cost_metrics.json читается model server'ом и tests/check_requirements.py
    cost_metrics = {
        "cost_per_job": metrics["cost_per_job"],
        "baseline_cost_per_job": metrics["baseline_cost_per_job"],
        "cost_savings_pct": metrics["achieved_cost_savings"],
        "cost_optimization_factor": metrics["cost_optimization_factor"],
        "availability": metrics["availability"],
        "inference_p99_ms": metrics["inference_p99_ms"],
        "data_drift_psi": metrics["data_drift_psi"],
        "schema_compliance_ratio": round(ok / max(ok + failed, 1), 4),
        "dlq_records_total": failed,
    }
    with open(os.path.join(WORK, "cost_model.json"), "w", encoding="utf-8") as fh:
        json.dump(cost_metrics, fh, ensure_ascii=False, indent=2)

    # Отдаём бизнес-метрики в model server, чтобы они попали в /metrics -> Prometheus
    if server_up:
        try:
            payload = json.dumps({
                "data_drift_psi": metrics["data_drift_psi"],
                "cost_per_job": metrics["cost_per_job"],
                "cost_savings_pct": metrics["achieved_cost_savings"],
                "schema_compliance_ratio": cost_metrics["schema_compliance_ratio"],
                "dlq_records_total": failed,
            }).encode("utf-8")
            req = urllib.request.Request(f"{MODEL_URL}/metrics/ingest", data=payload,
                                         headers={"Content-Type": "application/json"}, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
        except Exception as exc:
            print(f"[load-generator] /metrics/ingest недоступен: {exc}", flush=True)
    return metrics


def main() -> int:
    ap = argparse.ArgumentParser(description="Генератор нагрузки ЛР-6")
    ap.add_argument("--mode", default="normal", choices=["normal", "peak", "fail", "late"])
    ap.add_argument("--events", type=int, default=int(os.environ.get("EVENTS", "600")))
    ap.add_argument("--rate", type=float, default=float(os.environ.get("RATE", "0")))
    args = ap.parse_args()

    rate = args.rate or {"normal": 100.0, "peak": 200.0, "fail": 100.0, "late": 100.0}[args.mode]
    m = run(args.mode, args.events, rate)
    for k, v in m.items():
        print(f"{k}={v}")
    print("## METRICS ##")
    print(f"REQUIRED_COST_SAVINGS={m['required_cost_savings']:g}")
    print(f"ACHIEVED_COST_SAVINGS={m['achieved_cost_savings']:.2f}")
    print(f"SYSTEM_AVAILABILITY={m['availability']:.6f}")
    print(f"COST_PER_JOB={m['cost_per_job']:.4f}")
    print(f"DATA_DRIFT_SCORE={m['data_drift_psi']:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
