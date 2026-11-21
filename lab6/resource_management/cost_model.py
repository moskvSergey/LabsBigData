"""
ЛР-6. Модель стоимости и доступности (вариант 8).

Уникальное требование варианта: снизить затраты на 40% через комбинацию
on-demand и spot, не нарушив SLA (доступность >= 99.5%, инференс < 100 мс).

Модель детерминированная и офлайн: считает cost per job, экономию,
cost optimization factor и доступность. Используется и в демо-стенде
(`load_generator`), и в автотестах (`tests/`), и в отчёте — цифры в README
берутся отсюда, а не «рисуются» руками.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.environ.get("COST_MODEL_CONFIG") or os.path.join(HERE, "cost_model.json")


@dataclass
class CostReport:
    """Результат одного расчёта: затраты, экономия, доступность."""

    baseline_cost: float
    optimized_cost: float
    baseline_cost_per_job: float
    optimized_cost_per_job: float
    cost_savings_pct: float
    cost_optimization_factor: float
    availability: float
    spot_fraction: float
    spot_hours: float
    on_demand_hours: float
    details: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "baseline_cost": round(self.baseline_cost, 4),
            "optimized_cost": round(self.optimized_cost, 4),
            "baseline_cost_per_job": round(self.baseline_cost_per_job, 4),
            "optimized_cost_per_job": round(self.optimized_cost_per_job, 4),
            "cost_savings_pct": round(self.cost_savings_pct, 2),
            "cost_optimization_factor": round(self.cost_optimization_factor, 3),
            "availability": round(self.availability, 6),
            "spot_fraction": self.spot_fraction,
            "spot_hours": round(self.spot_hours, 2),
            "on_demand_hours": round(self.on_demand_hours, 2),
            **self.details,
        }


def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _executor_cost(hours: float, spot_fraction: float, cfg: Dict[str, Any]) -> float:
    """Стоимость executor-часов при заданной доле spot."""
    price = cfg["instance"]
    on_demand_hours = hours * (1.0 - spot_fraction)
    spot_hours = hours * spot_fraction
    return (
        on_demand_hours * price["on_demand_usd_per_hour"]
        + spot_hours * price["spot_usd_per_hour"]
    )


def calculate_cost(
    executor_hours: float,
    spot_fraction: float,
    driver_hours: float = 0.0,
    cfg: Dict[str, Any] | None = None,
) -> float:
    """Стоимость одного запуска задачи, USD.

    Драйвер всегда on-demand (его падение стоит дороже экономии на spot).
    """
    cfg = cfg or load_config()
    price = cfg["instance"]["on_demand_usd_per_hour"]
    return _executor_cost(executor_hours, spot_fraction, cfg) + driver_hours * price


def calculate_availability(
    spot_interruptions_per_node: float,
    nodes: int,
    recovery_seconds: float,
    seconds_per_month: float,
) -> float:
    """Доступность сервиса с учётом вытеснения spot-узлов.

    Модель консервативная: считаем, что каждое вытеснение даёт простой
    длиной `recovery_seconds` (перезапуск пода + переподписка на checkpoint).
    Репликация и распределение по зонам уже учтены в `nodes` — сервис
    продолжает отвечать, пока жива хотя бы часть реплик.
    """
    downtime = spot_interruptions_per_node * nodes * recovery_seconds
    return max(0.0, 1.0 - downtime / seconds_per_month)


def simulate(cfg: Dict[str, Any] | None = None) -> CostReport:
    """Полный расчёт: baseline vs оптимизированный вариант."""
    cfg = cfg or load_config()
    job = cfg["job"]
    hours = float(job["executor_hours_per_run"])
    driver_hours = float(job["driver_on_demand_hours_per_run"])

    baseline_spot = float(cfg["baseline"]["spot_fraction"])
    opt = cfg["optimized"]
    opt_spot = float(opt["spot_fraction"])

    # Fault tolerance на spot стоит дополнительного времени на пересчёт
    # упавших задач (checkpoint + повторный запуск stage).
    opt_hours = hours * (1.0 + float(opt["spot_fault_tolerance_overhead"]))

    baseline_cost = calculate_cost(hours, baseline_spot, driver_hours, cfg)
    optimized_cost = calculate_cost(opt_hours, opt_spot, driver_hours, cfg)

    savings = (baseline_cost - optimized_cost) / baseline_cost * 100.0
    factor = baseline_cost / optimized_cost if optimized_cost else float("inf")

    availability = calculate_availability(
        spot_interruptions_per_node=float(cfg["spot_interruptions_per_node_per_month"]),
        nodes=int(opt["nodes"]),
        recovery_seconds=float(opt["recovery_seconds_per_interruption"]),
        seconds_per_month=float(cfg["seconds_per_month"]),
    )

    runs = int(job["runs_per_day"]) * int(cfg["history_days"])

    return CostReport(
        baseline_cost=baseline_cost,
        optimized_cost=optimized_cost,
        baseline_cost_per_job=baseline_cost,
        optimized_cost_per_job=optimized_cost,
        cost_savings_pct=savings,
        cost_optimization_factor=factor,
        availability=availability,
        spot_fraction=opt_spot,
        spot_hours=opt_hours * opt_spot,
        on_demand_hours=opt_hours * (1.0 - opt_spot),
        details={
            "runs": runs,
            "baseline_cost_total": round(baseline_cost * runs, 2),
            "optimized_cost_total": round(optimized_cost * runs, 2),
            "saved_total": round((baseline_cost - optimized_cost) * runs, 2),
            "executor_hours": hours,
            "optimized_executor_hours": round(opt_hours, 2),
        },
    )


def main() -> None:
    report = simulate()
    print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
