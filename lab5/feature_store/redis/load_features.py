"""
Загрузка фиче-стора в Redis (online-фичи для потокового инференса).

Читает work/feature_store.json (готовит model/train_export.py) и раскладывает:
    feat:user:<userId>   -> hash {user_avg_rating, user_rating_count, user_std_rating}
    feat:movie:<movieId> -> hash {movie_avg_rating, movie_rating_count, movie_year}
    feat:global          -> hash {user_avg_rating, movie_avg_rating}  (холодный старт)

Запуск (локально):
    python feature_store/redis/load_features.py --host localhost
Запуск (в docker-compose):
    python load_features.py --host redis
"""

import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LAB5 = os.path.dirname(os.path.dirname(HERE))
FEATURE_DUMP = os.path.join(LAB5, "work", "feature_store.json")

BATCH = 2000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--dump", default=FEATURE_DUMP)
    args = ap.parse_args()

    if not os.path.exists(args.dump):
        sys.exit(f"Нет файла фиче-стора {args.dump}. Сначала: python model/train_export.py")

    t0 = time.time()
    with open(args.dump, encoding="utf-8") as f:
        data = json.load(f)
    print(f"[1] Фиче-стор прочитан за {time.time() - t0:.1f} c: "
          f"{len(data['users']):,} пользователей, {len(data['movies']):,} фильмов", flush=True)

    import redis

    r = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    r.ping()
    print(f"[2] Redis {args.host}:{args.port} доступен, версия {r.info()['redis_version']}", flush=True)

    r.hset("feat:global", mapping={
        "user_avg_rating": data["global_user_avg"],
        "movie_avg_rating": data["global_movie_avg"],
    })

    t0 = time.time()
    pipe = r.pipeline(transaction=False)
    n = 0
    for u in data["users"]:
        pipe.hset(f"feat:user:{u['userId']}", mapping={
            "user_avg_rating": u["user_avg_rating"],
            "user_rating_count": u["user_rating_count"],
            "user_std_rating": u["user_std_rating"],
        })
        n += 1
        if n % BATCH == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()
    print(f"[3] Пользователи загружены: {n:,} за {time.time() - t0:.1f} c", flush=True)

    t0 = time.time()
    pipe = r.pipeline(transaction=False)
    n = 0
    for m in data["movies"]:
        pipe.hset(f"feat:movie:{m['movieId']}", mapping={
            "movie_avg_rating": m["movie_avg_rating"],
            "movie_rating_count": m["movie_rating_count"],
            "movie_year": m["movie_year"],
        })
        n += 1
        if n % BATCH == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()
    print(f"[4] Фильмы загружены: {n:,} за {time.time() - t0:.1f} c", flush=True)

    total = len([k for k in r.scan_iter("feat:*", count=5000)])
    print(f"[5] Ключей feat:* в Redis: {total:,}", flush=True)
    print("## FEATURE_STORE ##", flush=True)
    print(f"USERS={len(data['users'])}", flush=True)
    print(f"MOVIES={len(data['movies'])}", flush=True)
    print(f"REDIS_KEYS={total}", flush=True)


if __name__ == "__main__":
    main()
