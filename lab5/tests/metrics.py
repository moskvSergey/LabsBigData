"""
Фиксированные метрики в выводе (требование к отчёту ЛР-5).

Печатает блок ## METRICS ##, который используется автоматическими проверками
и приложением к отчёту.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common  # noqa: E402


def print_metrics(m: dict):
    print("## METRICS ##")
    print(f"REQUIRED_LATENCY={m['required_latency_ms']:g}")
    print(f"MEASURED_P50_LATENCY={m['measured_p50_latency_ms']:.2f}")
    print(f"MEASURED_P95_LATENCY={m['measured_p95_latency_ms']:.2f}")
    print(f"MEASURED_P99_LATENCY={m['measured_p99_latency_ms']:.2f}")
    print(f"SLA_OK={m['sla_ok']}")
    print(f"LATE_DATA_HANDLED={m['late_data_handled']}")
    print(f"LATE_EVENTS={m['late_events']}")
    print(f"PREDICTIONS={m['predictions']}")
    print(f"DUPLICATES={m['duplicates']}")
    print(f"ERRORS={m['errors']}")
    print(f"THROUGHPUT_EPS={m['throughput_eps']}")
    print(f"CHECKPOINT_DURATION_SEC={m['checkpoint_duration_sec']:.2f}")
    print(f"DATA_DRIFT_SCORE={m['data_drift_psi']:.4f}")


def main():
    try:
        m = common.load_metrics()
    except FileNotFoundError:
        print(f"Нет {common.METRICS_JSON}. Дождитесь, пока сервис monitor закончит замер.")
        sys.exit(2)
    print_metrics(m)


if __name__ == "__main__":
    main()
