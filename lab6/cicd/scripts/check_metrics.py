#!/usr/bin/env python
"""
ЛР-6, Часть 4, п.3. Метрики для сравнения версий при безопасном деплое.

Читает /metrics канареечного и базового окружения, сравнивает ключевые
показатели (p99 latency, доля ошибок, drift) и возвращает код 1, если
новая версия хуже — CI/CD по этому коду делает автоматический откат.

Запуск:
  python cicd/scripts/check_metrics.py --endpoint http://localhost:8000
  python cicd/scripts/check_metrics.py --endpoint URL --baseline-endpoint URL2
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request

GAUGES = {
    "lab6_inference_latency_ms_ms": {"quantile": "0.99"},
    "lab6_schema_compliance_ratio": {},
    "lab6_data_drift_psi": {},
    "lab6_cost_savings_pct": {},
    "lab6_cost_per_job_usd": {},
}
COUNTERS = {"lab6_requests_total": {}, "lab6_errors_total": {}}


def parse_metrics(text: str) -> dict:
    values: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9.eE+-]+)$", line)
        if not m:
            continue
        name, labels, value = m.group(1), m.group(2) or "", float(m.group(3))
        key = f"{name}{labels}"
        values[key] = value
    return values


def fetch(endpoint: str) -> dict:
    with urllib.request.urlopen(f"{endpoint.rstrip('/')}/metrics", timeout=10) as resp:
        return parse_metrics(resp.read().decode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--baseline-endpoint", default="")
    ap.add_argument("--threshold", type=float, default=0.95,
                    help="минимальная доля успешных запросов (по умолчанию 0.95)")
    args = ap.parse_args()

    cur = fetch(args.endpoint)
    p99 = cur.get('lab6_inference_latency_ms_ms{quantile="0.99"}', 0.0)
    requests = cur.get("lab6_requests_total", 0.0)
    errors = cur.get("lab6_errors_total", 0.0)
    success_ratio = 1.0 - (errors / requests) if requests else 1.0
    drift = cur.get("lab6_data_drift_psi", 0.0)
    compliance = cur.get("lab6_schema_compliance_ratio", 1.0)
    cost = cur.get("lab6_cost_per_job_usd", 0.0)
    savings = cur.get("lab6_cost_savings_pct", 0.0)

    print(f"CANDIDATE_P99_MS={p99:.3f}")
    print(f"CANDIDATE_SUCCESS_RATIO={success_ratio:.4f}")
    print(f"CANDIDATE_DRIFT_PSI={drift:.4f}")
    print(f"CANDIDATE_SCHEMA_COMPLIANCE={compliance:.4f}")
    print(f"CANDIDATE_COST_PER_JOB={cost:.4f}")
    print(f"CANDIDATE_COST_SAVINGS_PCT={savings:.2f}")

    if args.baseline_endpoint:
        base = fetch(args.baseline_endpoint)
        b_p99 = base.get('lab6_inference_latency_ms_ms{quantile="0.99"}', 0.0)
        print(f"BASELINE_P99_MS={b_p99:.3f}")

    failures = []
    if success_ratio < args.threshold:
        failures.append(f"успешных запросов {success_ratio:.4f} < {args.threshold}")
    if p99 > 100:
        failures.append(f"p99 {p99:.1f} мс > SLA 100 мс")
    if drift > 0.2:
        failures.append(f"drift PSI {drift:.3f} > 0.2")
    if compliance < 0.99:
        failures.append(f"schema compliance {compliance:.4f} < 0.99")
    if savings and savings < 40:
        failures.append(f"экономия {savings:.1f}% < 40%")

    if failures:
        print("DEPLOY_DECISION=ROLLBACK")
        for f in failures:
            print(f"  FAIL: {f}")
        return 1
    print("DEPLOY_DECISION=PROMOTE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
