"""
Проверка обработки late-arriving данных.

Сценарий:
  1. Отправляем «свежие» события — они должны попасть в окно и дать предикты;
  2. Отправляем события с event_time на 6 минут в прошлом (позже
     watermark 5 c + allowedLateness 5 мин) — они должны быть перехвачены
     side output'ом и попасть в dlq_late (или быть помечены is_late=true).

Запуск: python -m pytest -v -s test_late_data.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common  # noqa: E402


def test_late_data_handling():
    """Late-arriving события доходят до пайплайна и учитываются отдельно."""
    producer = common.make_producer()

    # 1) свежие события
    common.send_events(producer, 100, late=False, user_id=9001, movie_id=9001)
    time.sleep(3)

    # 2) late-arriving: event_time на 6 минут в прошлом (позже allowedLateness 5 мин)
    common.send_events(producer, 50, late=True, late_delay_s=360.0,
                       user_id=9002, movie_id=9002)
    producer.close()

    n_pred_late, n_dlq_late = common.count_late_records(timeout_s=60)
    total_late = n_pred_late + n_dlq_late
    print(f"\n[late] помечено is_late в predictions: {n_pred_late}, "
          f"в dlq_late: {n_dlq_late}, всего: {total_late}")

    assert total_late > 0, (
        "late-arriving события не были обработаны: ни is_late в predictions, "
        "ни записей в dlq_late"
    )


def test_late_data_in_monitor_metrics():
    """Сводные метрики подтверждают, что late-данные обработаны."""
    m = common.load_metrics()
    print(f"\n[monitor] late_events={m['late_events']} "
          f"late_data_handled={m['late_data_handled']}")
    assert m["late_data_handled"], "monitor не зафиксировал late-arriving события"
