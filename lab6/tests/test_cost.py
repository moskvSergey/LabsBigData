"""
ЛР-6, Часть 6. Проверка уникального требования варианта 8:
снижение затрат на 40% через комбинацию on-demand и spot — без ущерба для SLA.

Запуск:  python tests/test_cost.py
Также собирается pytest'ом (test_* функции).
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common  # noqa: E402


def test_cost_optimization() -> None:
    """Проверка уникального требования по cost optimization (вариант 8)."""
    m = common.load_metrics()
    savings = m["achieved_cost_savings"]
    assert savings >= common.REQUIRED_SAVINGS, (
        f"Экономия {savings:.2f}% < {common.REQUIRED_SAVINGS:.0f}%"
    )
    print(f"PASS test_cost_optimization: экономия {savings:.2f}% >= 40%")


def test_spot_fraction() -> None:
    """Смешанная стратегия: spot должен давать основную часть ёмкости."""
    m = common.load_metrics()
    assert 0.5 <= m["spot_fraction"] <= 0.85, (
        f"Доля spot {m['spot_fraction']:.2f} вне рабочего диапазона 0.5..0.85"
    )
    print(f"PASS test_spot_fraction: {m['spot_fraction']:.0%} executor-времени на spot")


def test_cost_per_job_below_baseline() -> None:
    """Cost per job должен быть заметно ниже baseline."""
    m = common.load_metrics()
    assert m["cost_per_job"] < m["baseline_cost_per_job"], (
        f"cost per job ${m['cost_per_job']} не ниже baseline ${m['baseline_cost_per_job']}"
    )
    assert m["cost_optimization_factor"] > 1.5, (
        f"cost optimization factor {m['cost_optimization_factor']} <= 1.5"
    )
    print(
        f"PASS test_cost_per_job_below_baseline: "
        f"${m['baseline_cost_per_job']:.2f} -> ${m['cost_per_job']:.2f} "
        f"(factor {m['cost_optimization_factor']}x)"
    )


def test_fault_tolerance_configured() -> None:
    """Spot без fault tolerance недопустим: overhead должен быть учтён в модели."""
    cfg = common.cost_model.load_config()
    overhead = cfg["optimized"]["spot_fault_tolerance_overhead"]
    assert overhead > 0, "spot-стратегия без учёта пересчёта упавших задач"
    assert cfg["optimized"]["driver_spot_fraction"] == 0.0, "драйвер обязан быть on-demand"
    print(f"PASS test_fault_tolerance_configured: overhead {overhead:.0%}, driver на on-demand")


def main() -> int:
    tests = [
        test_cost_optimization,
        test_spot_fraction,
        test_cost_per_job_below_baseline,
        test_fault_tolerance_configured,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as exc:
            print(f"FAIL {t.__name__}: {exc}")
            failed += 1
    print(f"COST_TESTS_FAILED={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
