"""
Генератор нагрузки: поток событий-оценок MovieLens в Kafka.

Каждое событие содержит:
    event_id    — уникальный id (нужен для проверки идемпотентности)
    user_id     — пользователь
    movie_id    — фильм
    rating      — оценка (ground truth, для проверки качества онлайн-модели)
    event_time  — время события, epoch ms (может быть в прошлом -> late-arriving)
    sent_ts     — момент отправки в топик, epoch ms (для end-to-end latency)
    is_late     — флаг late-arriving события

Режимы:
    normal  — номинальная нагрузка
    peak    — пиковая (x2 от номинальной)
    late    — добавляет события с event_time на N минут в прошлом
    burst   — короткий всплеск на максимальной скорости

Запуск:
    python load_generator/generator.py --rate 200 --count 2000
    python load_generator/generator.py --mode late --rate 200
    python load_generator/generator.py --sink file --out work/events.jsonl --count 500
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
LAB5 = os.path.dirname(HERE)
ROOT = os.path.dirname(LAB5)
RATINGS = os.environ.get(
    "RATINGS_PATH", os.path.join(ROOT, "lab2", "work", "ratings_100mb.parquet")
)

TOPIC_EVENTS = "rating_events"
DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def now_ms() -> float:
    return time.time() * 1000.0


def load_events(n: int, seed: int = 42):
    """Берём реальные оценки MovieLens — это и есть наш датасет из ЛР-1."""
    import pandas as pd

    if not os.path.exists(RATINGS):
        sys.exit(f"Не найден {RATINGS} — сначала выполните ЛР-2.")
    df = pd.read_parquet(RATINGS, columns=["userId", "movieId", "rating", "timestamp"])
    df = df.sample(n=min(n, len(df)), random_state=seed).reset_index(drop=True)
    return df


def make_event(row, is_late: bool, late_delay_s: float) -> dict:
    base_ms = now_ms()
    event_ms = base_ms - (late_delay_s * 1000.0 if is_late else 0.0)
    return {
        "event_id": uuid.uuid4().hex,
        "user_id": int(row.userId),
        "movie_id": int(row.movieId),
        "rating": float(row.rating),
        "event_time": int(event_ms),
        "sent_ts": round(base_ms, 3),
        "is_late": bool(is_late),
        "source_time": int(row.timestamp) * 1000,
    }


def build_producer(bootstrap: str):
    from kafka import KafkaProducer

    # Сериализуем вручную (bytes): API сериализаторов менялся между версиями
    # kafka-python, а нам нужна совместимость и минимальная задержка.
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        # acks=1 + linger_ms=0: минимальная задержка продюсера (важно для p99)
        acks=1,
        linger_ms=0,
        retries=3,
        max_in_flight_requests_per_connection=1,
    )


def wait_for_flink(url: str, timeout_s: int = 180) -> bool:
    """Ждём, пока в кластере Flink появится RUNNING-задание.

    Без этого генератор начинает слать события раньше, чем Kafka-источник
    задания встал на offsets=latest, и эти события теряются.
    """
    import urllib.request

    deadline = time.time() + timeout_s
    print(f"[0] Ожидание RUNNING-задания Flink ({url})...", flush=True)
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url.rstrip("/") + "/jobs/overview", timeout=5) as r:
                data = json.loads(r.read().decode("utf-8"))
            running = [j for j in data.get("jobs", []) if j.get("state") == "RUNNING"]
            if running:
                print(f"[0] Задание {running[0]['jid']} в состоянии RUNNING, "
                      f"даём 8 с на прогрев", flush=True)
                time.sleep(8)
                return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(3)
    print("[0] Flink не поднялся за отведённое время — продолжаем", flush=True)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--wait-for-flink", default=os.environ.get("WAIT_FOR_FLINK", ""))
    ap.add_argument("--topic", default=TOPIC_EVENTS)
    ap.add_argument("--rate", type=float, default=200.0, help="событий в секунду")
    ap.add_argument("--count", type=int, default=2000, help="сколько событий отправить (0 = бесконечно)")
    ap.add_argument("--mode", choices=["normal", "peak", "late", "burst"], default="normal")
    ap.add_argument("--late-fraction", type=float, default=0.05, help="доля late-arriving событий")
    ap.add_argument("--late-delay-sec", type=float, default=360.0, help="на сколько опаздывают события")
    ap.add_argument("--sink", choices=["kafka", "file"], default="kafka")
    ap.add_argument("--out", default=os.path.join(LAB5, "work", "events.jsonl"))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--label", default="", help="метка прогона (для отчёта)")
    args = ap.parse_args()

    rate = args.rate * (2.0 if args.mode == "peak" else 1.0)
    n = args.count if args.count > 0 else 10 ** 9

    if args.wait_for_flink:
        wait_for_flink(args.wait_for_flink)

    print(f"[1] Режим={args.mode} rate={rate:.0f} событий/с count={args.count or 'inf'} "
          f"sink={args.sink} late_fraction={args.late_fraction}", flush=True)

    df = load_events(max(n if n < 10 ** 9 else 20000, 1000), seed=args.seed)
    print(f"[2] Загружено {len(df):,} реальных оценок MovieLens для генерации потока", flush=True)

    producer = None
    fout = None
    if args.sink == "kafka":
        producer = build_producer(args.bootstrap)
        print(f"[3] Продюсер подключён к {args.bootstrap}, топик {args.topic}", flush=True)
    else:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        fout = open(args.out, "w", encoding="utf-8")
        print(f"[3] Пишем события в файл {args.out}", flush=True)

    rng = random.Random(args.seed)
    sent = 0
    late_sent = 0
    t_start = time.perf_counter()
    interval = 1.0 / rate if args.mode != "burst" else 0.0
    next_t = t_start
    latencies = []

    try:
        for i in range(n):
            row = df.iloc[i % len(df)]
            is_late = rng.random() < args.late_fraction
            ev = make_event(row, is_late, args.late_delay_sec)
            payload = json.dumps(ev).encode("utf-8")

            t0 = time.perf_counter()
            if producer is not None:
                producer.send(args.topic, payload)
            else:
                fout.write(payload.decode("utf-8") + "\n")
            latencies.append((time.perf_counter() - t0) * 1000)

            sent += 1
            late_sent += int(is_late)

            if interval > 0:
                next_t += interval
                sleep_for = next_t - time.perf_counter()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                else:
                    next_t = time.perf_counter()

            if sent % 500 == 0:
                el = time.perf_counter() - t_start
                print(f"    отправлено {sent:,} ({sent / el:.0f} соб/с), late={late_sent}", flush=True)
    except KeyboardInterrupt:
        print("    прервано пользователем", flush=True)
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        if fout is not None:
            fout.close()

    el = time.perf_counter() - t_start
    lat = sorted(latencies)
    p50 = lat[int(len(lat) * 0.5)] if lat else 0.0
    p99 = lat[min(int(len(lat) * 0.99), len(lat) - 1)] if lat else 0.0
    print(f"[4] Отправлено {sent:,} событий за {el:.2f} c "
          f"({sent / el:.0f} событий/с), из них late: {late_sent}", flush=True)
    print("## LOAD_GENERATOR ##", flush=True)
    print(f"EVENTS_SENT={sent}", flush=True)
    print(f"LATE_SENT={late_sent}", flush=True)
    print(f"THROUGHPUT_EPS={sent / el:.1f}", flush=True)
    print(f"SEND_P50_MS={p50:.3f}", flush=True)
    print(f"SEND_P99_MS={p99:.3f}", flush=True)
    print(f"WALL_SEC={el:.2f}", flush=True)
    if args.label:
        print(f"LABEL={args.label}", flush=True)


if __name__ == "__main__":
    main()
