#!/usr/bin/env python
"""
ЛР-6, Часть 4, п.4. Smoke-тесты перед продовым деплоем.

Проверяет, что канареечная версия вообще жива и отвечает корректно:
  /health -> 200, /predict -> score в [0, 1], /predict/batch -> длина совпадает.
Возвращает 0 (можно катить на 100%) или 1 (откат).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

SAMPLE = {
    "event_id": "smoke-1",
    "user_id": 196,
    "movie_id": 242,
    "rating": 3.0,
    "user_avg_rating": 3.71,
    "movie_avg_rating": 3.33,
    "user_rating_count": 105,
}


def _post(endpoint: str, path: str, payload):
    req = urllib.request.Request(
        f"{endpoint.rstrip('/')}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run(endpoint: str) -> int:
    checks: list[tuple[str, bool, str]] = []

    try:
        with urllib.request.urlopen(f"{endpoint.rstrip('/')}/health", timeout=10) as resp:
            health = json.loads(resp.read().decode("utf-8"))
        checks.append(("health", resp.status == 200 and health.get("status") == "ok", str(health)))
    except (urllib.error.URLError, OSError) as exc:
        checks.append(("health", False, str(exc)))
        health = {}

    try:
        single = _post(endpoint, "/predict", SAMPLE)
        ok = 0.0 <= float(single.get("score", -1)) <= 1.0
        checks.append(("predict", ok, f"score={single.get('score')}"))
    except Exception as exc:
        checks.append(("predict", False, str(exc)))

    try:
        batch = _post(endpoint, "/predict/batch", [SAMPLE, SAMPLE])
        checks.append(("predict/batch", isinstance(batch, list) and len(batch) == 2, f"len={len(batch)}"))
    except Exception as exc:
        checks.append(("predict/batch", False, str(exc)))

    failed = 0
    for name, ok, detail in checks:
        print(f"SMOKE_{name.replace('/', '_').upper()}={'PASS' if ok else 'FAIL'} ({detail})")
        failed += 0 if ok else 1
    print(f"SMOKE_RESULT={'PASS' if failed == 0 else 'FAIL'}")
    return 0 if failed == 0 else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--wait", type=int, default=0, help="сколько секунд ждать готовности")
    args = ap.parse_args()

    deadline = time.time() + args.wait
    while True:
        code = run(args.endpoint)
        if code == 0 or time.time() >= deadline:
            return code
        time.sleep(5)


if __name__ == "__main__":
    raise SystemExit(main())
