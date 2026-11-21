"""
ЛР-6. Печать фиксированных метрик в формате, требуемом заданием:
  ## METRICS ##
  REQUIRED_COST_SAVINGS / ACHIEVED_COST_SAVINGS / SYSTEM_AVAILABILITY /
  COST_PER_JOB / DATA_DRIFT_SCORE

Запуск:  python tests/metrics.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import check_requirements  # noqa: E402
import common  # noqa: E402


def print_metrics(m: dict) -> None:
    check_requirements.print_metrics(m)


def main() -> int:
    try:
        m = common.load_metrics()
    except Exception as exc:
        print(f"Не удалось собрать метрики: {exc}")
        return 2
    print_metrics(m)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
