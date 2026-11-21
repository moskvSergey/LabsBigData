"""
ЛР-6, Часть 6, п.3. Проверка SLA: доступность >= 99.5% и инференс < 100 мс.

Запуск:  python tests/test_sla.py
Также собирается pytest'ом.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common  # noqa: E402


def test_sla() -> None:
    """Проверка SLA по доступности (вариант 8, сценарий «рекомендации»)."""
    m = common.load_metrics()
    availability = m["availability"]
    assert availability >= common.REQUIRED_AVAILABILITY, (
        f"Доступность {availability:.4f} < {common.REQUIRED_AVAILABILITY:.1%}"
    )
    print(f"PASS test_sla: доступность {availability:.4%} >= 99.5%")


def test_inference_latency() -> None:
    """Задержка инференса: требование сценария < 100 мс."""
    m = common.load_metrics()
    if not m["load_test_present"]:
        print("SKIP test_inference_latency: work/load_test.json ещё не создан "
              "(запустите `make load-test`)")
        return
    p99 = m["inference_p99_ms"]
    assert p99 < common.REQUIRED_LATENCY_MS, f"p99 {p99:.2f} мс >= {common.REQUIRED_LATENCY_MS} мс"
    print(f"PASS test_inference_latency: p99 {p99:.2f} мс < 100 мс")


def test_no_lost_requests_under_failure() -> None:
    """Отказ узла не должен приводить к потерянным запросам (retry + репликация)."""
    m = common.load_metrics()
    if not m["load_test_present"]:
        print("SKIP test_no_lost_requests_under_failure: нет живого прогона")
        return
    total = m["events_ok"] + m["events_failed"]
    lost_ratio = m["events_failed"] / max(total, 1)
    assert lost_ratio < 0.01, f"потеряно {lost_ratio:.2%} запросов (порог 1%)"
    print(f"PASS test_no_lost_requests_under_failure: потерь {lost_ratio:.2%} < 1%")


def test_availability_model_beats_sla() -> None:
    """Модель доступности при вытеснении spot должна проходить SLA."""
    report = common.cost_model.simulate()
    assert report.availability >= common.REQUIRED_AVAILABILITY
    print(f"PASS test_availability_model_beats_sla: модель {report.availability:.4%}")


def main() -> int:
    tests = [
        test_sla,
        test_inference_latency,
        test_no_lost_requests_under_failure,
        test_availability_model_beats_sla,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as exc:
            print(f"FAIL {t.__name__}: {exc}")
            failed += 1
    print(f"SLA_TESTS_FAILED={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
