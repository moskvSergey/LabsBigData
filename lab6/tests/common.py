"""
ЛР-6. Общий контекст для тестов и метрик: единый источник цифр.

Источники (в порядке приоритета):
  1. work/load_test.json      — если генератор нагрузки уже отработал (живой замер);
  2. work/data_validation.json — результат проверки качества данных;
  3. resource_management/cost_model.py — детерминированная cost-модель (всегда есть).
"""

from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
WORK = os.environ.get("WORK_DIR") or os.path.join(ROOT, "work")

sys.path.insert(0, os.path.join(ROOT, "resource_management"))
import cost_model  # noqa: E402

REQUIRED_SAVINGS = 40.0
REQUIRED_AVAILABILITY = 0.995
REQUIRED_LATENCY_MS = 100.0


def _read(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_metrics() -> dict:
    """Собирает все метрики отчёта в один словарь."""
    report = cost_model.simulate()
    live = _read(os.path.join(WORK, "load_test.json"))
    validation = _read(os.path.join(WORK, "data_validation.json"))

    # Канонический PSI — офлайн-замер по всему срезу датасета (work/drift.json):
    # он устойчив, потому что не зависит от объёма одного нагрузочного прогона.
    # PSI живого прогона сохраняем отдельно: он виден в отчётах генератора.
    drift = _stored_drift()
    drift_live = float(live.get("data_drift_psi") or 0.0)
    if not drift:
        drift = drift_live

    availability = float(live.get("availability") or report.availability)
    p99 = float(live.get("inference_p99_ms") or 0.0)

    return {
        # --- требуемые к выводу метрики ---
        "required_cost_savings": REQUIRED_SAVINGS,
        "achieved_cost_savings": round(report.cost_savings_pct, 2),
        "availability": availability,
        "cost_per_job": round(report.optimized_cost_per_job, 4),
        "data_drift_score": round(drift, 4),
        "data_drift_score_live": round(drift_live, 4),
        # --- дополнительные ---
        "baseline_cost_per_job": round(report.baseline_cost_per_job, 4),
        "cost_optimization_factor": round(report.cost_optimization_factor, 3),
        "required_availability": REQUIRED_AVAILABILITY,
        "required_latency_ms": REQUIRED_LATENCY_MS,
        "inference_p99_ms": p99,
        "inference_p50_ms": float(live.get("inference_p50_ms") or 0.0),
        "throughput_eps": float(live.get("throughput_eps") or 0.0),
        "spot_fraction": report.spot_fraction,
        "total_saved_usd": report.details.get("saved_total", 0.0),
        "baseline_cost_total": report.details.get("baseline_cost_total", 0.0),
        "optimized_cost_total": report.details.get("optimized_cost_total", 0.0),
        "schema_compliance": float(validation.get("success_ratio") or 1.0),
        "load_test_present": bool(live),
        "load_test_mode": live.get("mode", ""),
        "events_ok": int(live.get("events_ok") or 0),
        "events_failed": int(live.get("events_failed") or 0),
    }


def _stored_drift() -> float:
    """PSI, посчитанный офлайн по срезу датасета (work/drift.json)."""
    stored = _read(os.path.join(WORK, "drift.json"))
    return float(stored.get("data_drift_psi") or 0.0)


def write_cost_metrics() -> str:
    """Публикует cost-метрики для model server и Grafana/Prometheus."""
    m = load_metrics()
    payload = {
        "cost_per_job": m["cost_per_job"],
        "baseline_cost_per_job": m["baseline_cost_per_job"],
        "cost_savings_pct": m["achieved_cost_savings"],
        "cost_optimization_factor": m["cost_optimization_factor"],
        "availability": m["availability"],
        "data_drift_psi": m["data_drift_score"],
        "schema_compliance_ratio": m["schema_compliance"],
        "inference_p99_ms": m["inference_p99_ms"],
    }
    os.makedirs(WORK, exist_ok=True)
    path = os.path.join(WORK, "cost_model.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return path
