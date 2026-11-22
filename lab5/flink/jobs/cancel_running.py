"""
Отменяет все RUNNING-задания этого пайплайна перед отправкой нового.

Нужно, чтобы `docker compose up` можно было выполнять повторно: иначе старое
и новое задания делят одну consumer-группу Kafka, и оба пишут в predictions.
"""

import json
import sys
import time
import urllib.request

JM = sys.argv[1] if len(sys.argv) > 1 else "http://flink-jobmanager:8081"
JOB_NAME = "lab5-streaming-recommender"


def get(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))


def main():
    try:
        overview = get(f"{JM}/jobs/overview")
    except Exception as exc:  # noqa: BLE001
        print(f"[cancel] JobManager недоступен: {exc}", flush=True)
        return

    for job in overview.get("jobs", []):
        if job.get("state") == "RUNNING" and JOB_NAME in job.get("name", ""):
            jid = job["jid"]
            try:
                req = urllib.request.Request(f"{JM}/jobs/{jid}?mode=cancel", method="PATCH")
                urllib.request.urlopen(req, timeout=10).read()
                print(f"[cancel] отменено задание {jid}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[cancel] не удалось отменить {jid}: {exc}", flush=True)
    time.sleep(3)


if __name__ == "__main__":
    main()
