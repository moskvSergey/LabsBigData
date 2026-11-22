"""
Лабораторная работа 5. Часть 3-4: обучение модели рекомендаций и экспорт в ONNX.

Сценарий: рекомендательная система онлайн-кинотеатра (MovieLens ml-32m, ЛР-1).
Задача: бинарная классификация — поставит ли пользователь высокую оценку (>= 4.0).

Модель намеренно компактная (логистическая регрессия): в потоке она вызывается
на каждое событие, поэтому важна не только AUC, но и latency инференса.

Запуск:
    python model/train_export.py
"""

import json
import os
import sys
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
LAB5 = os.path.dirname(HERE)
ROOT = os.path.dirname(LAB5)

# Датасет из ЛР-1 / ЛР-2 (срез 100 МБ, ~4 млн оценок)
RATINGS = os.path.join(ROOT, "lab2", "work", "ratings_100mb.parquet")
MOVIES = os.path.join(ROOT, "lab2", "work", "movies.parquet")

MODELS_DIR = os.path.join(LAB5, "models")
WORK_DIR = os.path.join(LAB5, "work")
FEATURE_DUMP = os.path.join(WORK_DIR, "feature_store.json")
MODEL_ONNX = os.path.join(MODELS_DIR, "recommender.onnx")

# Порог "понравилось"
LIKE_THRESHOLD = 4.0

FEATURE_NAMES = [
    "user_avg_rating",     # средняя оценка пользователя (история)
    "user_rating_count",   # активность пользователя
    "user_std_rating",     # разброс оценок пользователя
    "movie_avg_rating",    # средняя оценка фильма (история)
    "movie_rating_count",  # популярность фильма
    "movie_year",          # год выпуска
    "hour",                # час суток  — временной паттерн (уникальное требование ЛР-4)
    "day_of_week",         # день недели — временной паттерн
]


def log(msg):
    print(msg, flush=True)


def load_data():
    if not os.path.exists(RATINGS):
        sys.exit(f"Не найден {RATINGS}. Сначала выполните ЛР-2 (срез ratings_100mb.parquet).")
    ratings = pd.read_parquet(RATINGS)
    movies = pd.read_parquet(MOVIES)

    # Год выпуска вытаскиваем из названия: "Toy Story (1995)"
    movies["movie_year"] = (
        movies["title"].str.extract(r"\((\d{4})\)\s*$")[0].astype("float32")
    )
    movies["movie_year"] = movies["movie_year"].fillna(movies["movie_year"].median())

    ratings["event_time"] = pd.to_datetime(ratings["timestamp"], unit="s")
    ratings = ratings.sort_values("event_time").reset_index(drop=True)

    movies_small = movies[["movieId", "movie_year"]].astype({"movieId": "int32"})
    ratings = ratings.merge(movies_small, on="movieId", how="left")
    ratings["movie_year"] = ratings["movie_year"].fillna(ratings["movie_year"].median())
    return ratings


def build_offline_stats(train: pd.DataFrame):
    """Статистики пользователей и фильмов считаем ТОЛЬКО на train-части.

    Иначе получим утечку: в онлайн-фичах не может быть информации из будущего.
    """
    user_stats = train.groupby("userId")["rating"].agg(["mean", "count", "std"])
    movie_stats = train.groupby("movieId")["rating"].agg(["mean", "count"])

    user_stats.columns = ["user_avg_rating", "user_rating_count", "user_std_rating"]
    movie_stats.columns = ["movie_avg_rating", "movie_rating_count"]

    # Глобальные средние — холодный старт (cold start)
    g_user = float(train["rating"].mean())
    g_movie = float(train["rating"].mean())
    user_stats["user_std_rating"] = user_stats["user_std_rating"].fillna(0.0)
    return user_stats, movie_stats, g_user, g_movie


def add_features(df: pd.DataFrame, user_stats, movie_stats, g_user, g_movie) -> pd.DataFrame:
    out = df.merge(
        user_stats, left_on="userId", right_index=True, how="left"
    ).merge(movie_stats, left_on="movieId", right_index=True, how="left")

    # Холодный старт: неизвестный пользователь/фильм -> глобальное среднее
    out["user_avg_rating"] = out["user_avg_rating"].fillna(g_user)
    out["movie_avg_rating"] = out["movie_avg_rating"].fillna(g_movie)
    out["user_rating_count"] = out["user_rating_count"].fillna(0)
    out["user_std_rating"] = out["user_std_rating"].fillna(0)
    out["movie_rating_count"] = out["movie_rating_count"].fillna(0)

    out["hour"] = out["event_time"].dt.hour.astype("float32")
    out["day_of_week"] = out["event_time"].dt.dayofweek.astype("float32")
    return out


def main():
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(WORK_DIR, exist_ok=True)

    log("=" * 70)
    log("ЛР-5. Обучение модели для потокового инференса")
    log("=" * 70)

    t0 = time.time()
    ratings = load_data()
    log(f"[1] Загружено {len(ratings):,} оценок за {time.time() - t0:.1f} c")
    log(f"    Пользователей: {ratings['userId'].nunique():,}, фильмов: {ratings['movieId'].nunique():,}")
    log(f"    Период: {ratings['event_time'].min().date()} .. {ratings['event_time'].max().date()}")

    # Временной сплит: прошлое -> train, будущее -> test (как в реальном serve)
    split = int(len(ratings) * 0.8)
    train, test = ratings.iloc[:split].copy(), ratings.iloc[split:].copy()
    log(f"[2] Временной сплит: train={len(train):,} (до {train['event_time'].max().date()}), "
        f"test={len(test):,} (с {test['event_time'].min().date()})")

    user_stats, movie_stats, g_user, g_movie = build_offline_stats(train)
    log(f"[3] Фиче-стор: {len(user_stats):,} пользователей, {len(movie_stats):,} фильмов")

    train_f = add_features(train, user_stats, movie_stats, g_user, g_movie)
    test_f = add_features(test, user_stats, movie_stats, g_user, g_movie)

    train_f["label"] = (train_f["rating"] >= LIKE_THRESHOLD).astype(int)
    test_f["label"] = (test_f["rating"] >= LIKE_THRESHOLD).astype(int)

    X_train = train_f[FEATURE_NAMES].astype("float32").to_numpy()
    y_train = train_f["label"].to_numpy()
    X_test = test_f[FEATURE_NAMES].astype("float32").to_numpy()
    y_test = test_f["label"].to_numpy()
    log(f"[4] Доля положительного класса: train={y_train.mean():.1%}, test={y_test.mean():.1%}")

    # ---- Модель 1: Logistic Regression (baseline для потока) ----
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score, f1_score
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import make_pipeline

    t0 = time.time()
    lr = make_pipeline(
        StandardScaler(),
        LogisticRegression(max_iter=1000, random_state=42),
    )
    lr.fit(X_train, y_train)
    lr_prob_test = lr.predict_proba(X_test)[:, 1]
    lr_auc = roc_auc_score(y_test, lr_prob_test)

    # Порог классификации подбираем по train (не по test — иначе подгонка).
    from sklearn.metrics import precision_recall_curve

    p_tr, r_tr, thr_tr = precision_recall_curve(y_train, lr.predict_proba(X_train)[:, 1])
    f1_tr = np.divide(2 * p_tr * r_tr, p_tr + r_tr, out=np.zeros_like(p_tr), where=(p_tr + r_tr) > 0)
    best_thr = float(thr_tr[int(np.argmax(f1_tr[:-1]))]) if len(thr_tr) else 0.5
    lr_f1 = f1_score(y_test, (lr_prob_test >= best_thr).astype(int))
    lr_train_s = time.time() - t0
    log(f"[5] LogisticRegression: AUC={lr_auc:.4f} F1={lr_f1:.4f} "
        f"(порог={best_thr:.3f}, {lr_train_s:.1f} c)")
    log(f"    Распределение score на test: "
        f"p10={np.percentile(lr_prob_test, 10):.3f} p50={np.percentile(lr_prob_test, 50):.3f} "
        f"p90={np.percentile(lr_prob_test, 90):.3f}")

    # ---- Модель 2: XGBoost (сравнение) ----
    try:
        from xgboost import XGBClassifier

        t0 = time.time()
        xgb = XGBClassifier(
            n_estimators=100, max_depth=5, learning_rate=0.1,
            random_state=42, eval_metric="logloss", n_jobs=-1,
        )
        xgb.fit(X_train, y_train)
        xgb_auc = roc_auc_score(y_test, xgb.predict_proba(X_test)[:, 1])
        xgb_f1 = f1_score(y_test, (xgb.predict_proba(X_test)[:, 1] >= best_thr).astype(int))
        xgb_train_s = time.time() - t0
        log(f"[6] XGBoost:            AUC={xgb_auc:.4f} F1={xgb_f1:.4f} ({xgb_train_s:.1f} c)")
    except Exception as exc:  # noqa: BLE001
        log(f"[6] XGBoost недоступен ({exc}); продолжаем с LogisticRegression")
        xgb, xgb_auc, xgb_f1, xgb_train_s = None, -1.0, -1.0, 0.0

    # Для потока выбираем логистическую регрессию: разница в AUC мала,
    # а latency инференса на порядок ниже (важно для p99 < 45 мс).
    model = lr
    model_name = "LogisticRegression"
    auc, f1 = lr_auc, lr_f1
    log(f"[7] Для потока выбрана: {model_name} (AUC={auc:.4f})")

    # ---- Экспорт в ONNX ----
    from skl2onnx import to_onnx
    from skl2onnx.common.data_types import FloatTensorType

    # target_opset фиксируем: onnxruntime 1.20 (в образе Flink) не умеет opset 22,
    # а skl2onnx по умолчанию ставит самый новый. 17 поддерживают все рантаймы.
    onnx_model = to_onnx(
        model,
        initial_types=[("input", FloatTensorType([None, len(FEATURE_NAMES)]))],
        options={"zipmap": False},
        target_opset=17,
    )
    with open(MODEL_ONNX, "wb") as f:
        f.write(onnx_model.SerializeToString())
    log(f"[8] ONNX сохранён: {MODEL_ONNX} ({os.path.getsize(MODEL_ONNX) / 1024:.1f} КБ)")

    # ---- Проверка ONNX: совпадение с sklearn + latency ----
    import onnxruntime as ort

    sess = ort.InferenceSession(MODEL_ONNX, providers=["CPUExecutionProvider"])
    out_names = [o.name for o in sess.get_outputs()]
    prob_idx = out_names.index("probabilities") if "probabilities" in out_names else 0
    sample = X_test[:1000].astype("float32")
    onnx_scores = np.asarray(sess.run(None, {"input": sample})[prob_idx])[:, 1].ravel()
    sk_scores = model.predict_proba(sample)[:, 1]
    max_diff = float(np.abs(onnx_scores - sk_scores).max())

    lat = []
    one = sample[:1]
    for _ in range(300):
        t = time.perf_counter()
        sess.run(None, {"input": one})
        lat.append((time.perf_counter() - t) * 1000)
    lat = np.array(lat)
    log(f"[9] ONNX == sklearn: max|diff|={max_diff:.2e} (проверка эквивалентности); "
        f"инференс p50={np.percentile(lat, 50):.2f} мс p99={np.percentile(lat, 99):.2f} мс")
    if max_diff > 1e-4:
        sys.exit("ONNX-модель не совпадает со sklearn — экспорт некорректен")

    # ---- Дамп фиче-стора для Redis ----
    users = user_stats.reset_index()
    users.columns = ["userId", "user_avg_rating", "user_rating_count", "user_std_rating"]
    movies_out = movie_stats.reset_index()
    movies_out.columns = ["movieId", "movie_avg_rating", "movie_rating_count"]

    # годы выпуска добавим к фильмам
    mv = pd.read_parquet(MOVIES)
    mv["movie_year"] = mv["title"].str.extract(r"\((\d{4})\)\s*$")[0].astype("float32")
    mv["movie_year"] = mv["movie_year"].fillna(mv["movie_year"].median())
    movies_out = movies_out.merge(mv[["movieId", "movie_year"]], on="movieId", how="left")
    movies_out["movie_year"] = movies_out["movie_year"].fillna(float(mv["movie_year"].median()))

    with open(FEATURE_DUMP, "w", encoding="utf-8") as f:
        json.dump(
            {
                "global_user_avg": g_user,
                "global_movie_avg": g_movie,
                "feature_names": FEATURE_NAMES,
                "users": users.round(6).to_dict(orient="records"),
                "movies": movies_out.round(6).to_dict(orient="records"),
            },
            f,
            ensure_ascii=False,
        )
    log(f"[10] Фиче-стор выгружен: {FEATURE_DUMP} "
        f"({os.path.getsize(FEATURE_DUMP) / 1024 / 1024:.1f} МБ)")

    # ---- MLflow: версионирование модели ----
    mlflow_run_id = None
    try:
        import mlflow

        # sqlite-бэкенд: файловый (./mlruns) в MLflow 3.x объявлен устаревшим
        mlflow.set_tracking_uri(f"sqlite:///{os.path.join(LAB5, 'mlflow.db').replace(os.sep, '/')}")
        mlflow.set_experiment("lab5_streaming_recommender")
        with mlflow.start_run(run_name=f"{model_name}-onnx") as run:
            mlflow_run_id = run.info.run_id
            mlflow.log_params({
                "model": model_name,
                "features": ",".join(FEATURE_NAMES),
                "like_threshold": LIKE_THRESHOLD,
                "train_rows": len(train),
                "test_rows": len(test),
                "split": "time-based 80/20",
            })
            mlflow.log_metric("auc", auc)
            mlflow.log_metric("f1", f1)
            mlflow.log_metric("onnx_latency_p50_ms", float(np.percentile(lat, 50)))
            mlflow.log_metric("onnx_latency_p99_ms", float(np.percentile(lat, 99)))
            mlflow.log_artifact(MODEL_ONNX)
            mlflow.set_tags({"model_version": "v1", "serving": "onnxruntime"})
        log(f"[11] MLflow run: {mlflow_run_id}")
    except Exception as exc:  # noqa: BLE001
        log(f"[11] MLflow недоступен ({exc}); версия модели фиксируется файлом models/model_version.json")

    with open(os.path.join(MODELS_DIR, "model_version.json"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "model_version": "recommender.onnx:v1",
                "model_name": model_name,
                "auc": round(float(auc), 6),
                "f1": round(float(f1), 6),
                "decision_threshold": round(float(best_thr), 6),
                "mlflow_run_id": mlflow_run_id,
                "feature_names": FEATURE_NAMES,
                "like_threshold": LIKE_THRESHOLD,
                "onnx_latency_p50_ms": round(float(np.percentile(lat, 50)), 4),
                "onnx_latency_p99_ms": round(float(np.percentile(lat, 99)), 4),
                "trained_at": pd.Timestamp.utcnow().isoformat(),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    log("=" * 70)
    log(f"ИТОГО: {model_name} AUC={auc:.4f} F1={f1:.4f}, ONNX p99={np.percentile(lat, 99):.2f} мс")
    log("=" * 70)


if __name__ == "__main__":
    main()
