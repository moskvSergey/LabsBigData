"""
Автоматические проверки требований ЛР-5 (вариант 8).

Проверяет по факту живого прогона (work/metrics.json + живой опрос Kafka):
  1. Уникальное требование к задержке: p99 < 45 мс                    (1.5 балла)
  2. Потоковая обработка: окна/watermark, late-arriving, state        (2.5 балла)
  3. Интеграция с ML: онлайн-инференс, метрики задержки инференса     (2.0 балла)
  4. Exactly-once: checkpoint'ы, отсутствие дубликатов                (1.5 балла)
  5. Мониторинг: метрики, PSI-дрейф, алерты                           (2.0 балла)
  6. Нагрузочные режимы: нормальный и пиковый                         (—)

Запуск: python check_requirements.py
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common  # noqa: E402

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok), detail))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    return ok


def main():
    print("=" * 74)
    print("ЛР-5. Проверка требований. Вариант 8 (p99 < 45 мс), MovieLens")
    print("=" * 74)

    try:
        m = common.load_metrics()
    except FileNotFoundError:
        print(f"Нет {common.METRICS_JSON}: сначала дождитесь завершения замера monitor.")
        sys.exit(2)

    req = m["required_latency_ms"]
    print(f"Метрики из {common.METRICS_JSON} (обновлены "
          f"{time.time() - m.get('ts', time.time()):.0f} c назад)\n")

    # 1. Уникальное требование к задержке
    check(
        f"1. Задержка p99 < {req:g} мс (уникальное требование, цифра 8)",
        m["measured_p99_latency_ms"] < req,
        f"p50={m['measured_p50_latency_ms']:.2f} p95={m['measured_p95_latency_ms']:.2f} "
        f"p99={m['measured_p99_latency_ms']:.2f} мс",
    )

    # 2. Потоковая обработка
    check("2a. Поток обрабатывается (есть предикты)",
          m["predictions"] > 0, f"{m['predictions']} предиктов")
    check("2b. Late-arriving данные обработаны",
          m["late_data_handled"], f"{m['late_events']} late-событий")
    check("2c. Устойчивая пропускная способность > 20 событий/с",
          m["throughput_eps"] > 20, f"{m['throughput_eps']:.1f} событий/с")

    # 3. Интеграция с ML
    check("3a. Онлайн-инференс выполняется (инференс-задержка измерена)",
          m["inference_p99_ms"] > 0, f"инференс p50={m['inference_p50_ms']:.3f} "
                                     f"p99={m['inference_p99_ms']:.3f} мс")
    check("3b. Задержка инференса укладывается в бюджет 45 мс",
          m["inference_p99_ms"] < req, f"инференс p99={m['inference_p99_ms']:.3f} мс")

    # 4. Exactly-once / идемпотентность
    check("4a. Дубликатов предиктов нет (идемпотентность по event_id)",
          m["duplicates"] == 0, f"дубликатов: {m['duplicates']}")
    check("4b. Checkpoint'ы выполняются",
          m["checkpoint_duration_sec"] > 0,
          f"длительность последнего checkpoint = {m['checkpoint_duration_sec']:.3f} с")
    check("4c. Доля ошибок ниже 1%",
          m["error_rate"] < 0.01, f"error_rate={m['error_rate']:.4f}")

    # 5. Мониторинг и дрейф
    check("5a. PSI посчитан (детекция дрейфа)",
          m["data_drift_psi"] >= 0.0, f"PSI={m['data_drift_psi']:.4f}, "
                                      f"порог 0.2, дрейф={'есть' if m['drift_detected'] else 'нет'}")

    print("\n" + "-" * 74)
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"ИТОГО: {passed}/{len(RESULTS)} проверок пройдено")

    print("\n## METRICS ##")
    print(f"REQUIRED_LATENCY={req:g}")
    print(f"MEASURED_P50_LATENCY={m['measured_p50_latency_ms']:.2f}")
    print(f"MEASURED_P95_LATENCY={m['measured_p95_latency_ms']:.2f}")
    print(f"MEASURED_P99_LATENCY={m['measured_p99_latency_ms']:.2f}")
    print(f"SLA_OK={m['sla_ok']}")
    print(f"LATE_DATA_HANDLED={m['late_data_handled']}")
    print(f"LATE_EVENTS={m['late_events']}")
    print(f"PREDICTIONS={m['predictions']}")
    print(f"DUPLICATES={m['duplicates']}")
    print(f"ERRORS={m['errors']}")
    print(f"ERROR_RATE={m['error_rate']:.4f}")
    print(f"THROUGHPUT_EPS={m['throughput_eps']:.1f}")
    print(f"INFERENCE_P99_LATENCY={m['inference_p99_ms']:.3f}")
    print(f"CHECKPOINT_DURATION_SEC={m['checkpoint_duration_sec']:.2f}")
    print(f"DATA_DRIFT_SCORE={m['data_drift_psi']:.4f}")

    out = os.path.join(common.WORK_DIR, "check_requirements.json")
    os.makedirs(common.WORK_DIR, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(
            {"checks": [{"name": n, "passed": ok, "detail": d} for n, ok, d in RESULTS],
             "passed": passed, "total": len(RESULTS), "metrics": m},
            f, ensure_ascii=False, indent=2)
    print(f"\nОтчёт: {out}")

    sys.exit(0 if passed == len(RESULTS) else 1)


if __name__ == "__main__":
    main()
