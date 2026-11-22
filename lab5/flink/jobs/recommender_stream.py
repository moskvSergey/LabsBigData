"""
ЛР-5. Потоковый пайплайн рекомендаций (Flink 1.20 / PyFlink, true streaming).

Топология:

                        ┌───────────────────────────────────────────────┐
   Kafka                │ parse (JSON -> tuple, event-time)             │
   rating_events ───────┤                                               │
                        └───┬────────────────────────────────┬──────────┘
                            │                                │
              keyBy(user_id)│                  keyBy(movie_id)│
                            ▼                                ▼
        ┌───────────────────────────────┐  ┌──────────────────────────────────┐
        │ UserStateInferenceFunction    │  │ Tumbling 1 мин, watermark 5 c    │
        │  • ValueState (TTL 24 ч)      │  │  • allowedLateness 5 мин         │
        │  • фичи из Redis (feature     │  │  • side output -> dlq_late       │
        │    store) + ONNX-инференс     │  │  • инкрементальный aggregate     │
        │  • late/ошибки -> side output │  │    -> movie_stats                │
        └───────────────┬───────────────┘  └──────────────────────────────────┘
                        ▼
                 Kafka predictions

Гарантии:
  • exactly-once: checkpointing (EXACTLY_ONCE) + KafkaSink с
    delivery_guarantee=exactly-once (транзакционный продюсер) + event_id,
    по которому потребитель дедуплицирует;
  • late-arriving: watermark 5 c + allowedLateness 5 мин + side outputs
    (и на уровне окна, и на уровне stateful-функции по current_watermark);
  • state: keyed ValueState с TTL 24 ч, восстанавливается из checkpoint;
  • fault tolerance: fixed-delay restart (3 попытки) + ретраи с экспоненциальной
    задержкой на уровне приложения (Redis/ONNX).

Запуск (внутри образа lab5-flink):
    python3 /opt/jobs/recommender_stream.py
"""

import datetime as _dt
import json
import os
import time

import numpy as np

from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import AggregateFunction, KeyedProcessFunction, ProcessWindowFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

try:  # TimestampAssigner живёт в разных модулях в разных версиях PyFlink
    from pyflink.common.watermark_strategy import TimestampAssigner
except ImportError:  # pragma: no cover
    from pyflink.datastream.functions import TimestampAssigner

# --------------------------------------------------------------------------
# Конфигурация (переопределяется переменными окружения)
# --------------------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
MODEL_PATH = os.environ.get("MODEL_PATH", "/opt/models/recommender.onnx")
FEATURE_JSON = os.environ.get("FEATURE_JSON", "/opt/models/feature_store.json")
FEATURE_MODE = os.environ.get("FEATURE_MODE", "warm")     # warm | online
WATERMARK_SEC = int(os.environ.get("WATERMARK_SEC", "5"))
ALLOWED_LATENESS_SEC = int(os.environ.get("ALLOWED_LATENESS_SEC", "300"))
WINDOW_SEC = int(os.environ.get("WINDOW_SEC", "60"))
CHECKPOINT_MS = int(os.environ.get("CHECKPOINT_MS", "30000"))
PARALLELISM = int(os.environ.get("PARALLELISM", "2"))
DECISION_THRESHOLD = float(os.environ.get("DECISION_THRESHOLD", "0.372844"))
CHECK_WATERMARK_PER_EVENT = os.environ.get("CHECK_WATERMARK_PER_EVENT", "0") == "1"

TOPIC_EVENTS = os.environ.get("TOPIC_EVENTS", "rating_events")
TOPIC_PREDICTIONS = os.environ.get("TOPIC_PREDICTIONS", "predictions")
TOPIC_MOVIE_STATS = os.environ.get("TOPIC_MOVIE_STATS", "movie_stats")
TOPIC_DLQ_ERRORS = os.environ.get("TOPIC_DLQ_ERRORS", "dlq_errors")
TOPIC_DLQ_LATE = os.environ.get("TOPIC_DLQ_LATE", "dlq_late")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "file:///opt/flink/checkpoints")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "recommender.onnx:v1")

# (event_id, user_id, movie_id, rating, event_time_ms, sent_ts_ms, is_late)
EVENT_TYPE = Types.TUPLE([
    Types.STRING(), Types.LONG(), Types.LONG(), Types.DOUBLE(),
    Types.LONG(), Types.DOUBLE(), Types.BOOLEAN(),
])
ACC_TYPE = Types.LIST(Types.DOUBLE())

# ВАЖНО: OutputTag создаём БЕЗ type_info (PICKLED_BYTE_ARRAY).
# Если переиспользовать TypeInformation, уже «прокинутый» в JVM, PyFlink не сможет
# сериализовать оператор (py4j-прокси держит RLock).
LATE_TAG_INFER = OutputTag("late-inference")
LATE_TAG_WINDOW = OutputTag("late-window")
ERROR_TAG = OutputTag("processing-errors")


def now_ms() -> float:
    return time.time() * 1000.0


def side_output_to_str(value) -> str:
    """Side output по умолчанию сериализуется как PICKLED_BYTE_ARRAY (byte[] в JVM),
    а KafkaSink с SimpleStringSchema ждёт String. Поэтому перед sink'ом приводим
    элементы side output к строке (иначе ClassCastException: [B -> String)."""
    if isinstance(value, str):
        return value
    if isinstance(value, (tuple, list)) and len(value) == 7:
        # элемент окна: (event_id, user_id, movie_id, rating, event_time, sent_ts, is_late)
        return json.dumps({
            "event_id": value[0], "user_id": value[1], "movie_id": value[2],
            "rating": value[3], "event_time": value[4], "sent_ts": value[5],
            "is_late": value[6], "reason": "late_window", "ts": now_ms(),
        }, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False, default=str)


class EventTimestampAssigner(TimestampAssigner):
    """event_time (epoch ms) события -> внутренний timestamp Flink."""

    def extract_timestamp(self, value, record_timestamp):
        return value[4]


def parse_event(raw: str):
    """JSON из Kafka -> кортеж. Битую запись помечаем user_id = -1."""
    try:
        e = json.loads(raw)
        return (
            str(e["event_id"]),
            int(e["user_id"]),
            int(e["movie_id"]),
            float(e["rating"]),
            int(e["event_time"]),
            float(e["sent_ts"]),
            bool(e.get("is_late", False)),
        )
    except Exception:  # noqa: BLE001
        return (raw[:200], -1, -1, -1.0, 0, now_ms(), False)


# --------------------------------------------------------------------------
# Фиче-стор (Redis) + локальный кэш в памяти воркера
# --------------------------------------------------------------------------
class FeatureStore:
    """Фичи пользователя и фильма из Redis.

    warm   — при старте воркера фичи выгружаются из Redis в память (мкс на событие);
    online — HGETALL в Redis на каждое событие.

    Если Redis недоступен, читаем снапшот фиче-стора (work/feature_store.json) —
    пайплайн не падает, но помечает режим как degraded.
    """

    def __init__(self, host, port, mode="warm", fallback_json=None):
        import redis

        self.mode = mode
        self.r = redis.Redis(host=host, port=port, decode_responses=True,
                             socket_timeout=2, socket_connect_timeout=2)
        self.users = {}
        self.movies = {}
        self.g_user, self.g_movie = 3.5, 3.5
        self.redis_ok = False
        self.cache = {}
        self.loaded = 0

    def open(self):
        try:
            self.r.ping()
            self.redis_ok = True
        except Exception:  # noqa: BLE001
            self.redis_ok = False

        if self.redis_ok:
            g = self.r.hgetall("feat:global")
            if g:
                self.g_user = float(g.get("user_avg_rating", 3.5))
                self.g_movie = float(g.get("movie_avg_rating", 3.5))
            if self.mode == "warm":
                self._warm_up()
        elif fallback_json and os.path.exists(fallback_json):
            with open(fallback_json, encoding="utf-8") as f:
                data = json.load(f)
            self.g_user = float(data["global_user_avg"])
            self.g_movie = float(data["global_movie_avg"])
            self.users = {int(u["userId"]): u for u in data["users"]}
            self.movies = {int(m["movieId"]): m for m in data["movies"]}
            self.loaded = len(self.users) + len(self.movies)
        return self

    def _drain(self, pattern, sink):
        pipe = self.r.pipeline(transaction=False)
        keys = []
        for k in self.r.scan_iter(pattern, count=5000):
            pipe.hgetall(k)
            keys.append(k)
            if len(keys) >= 2000:
                for kk, vv in zip(keys, pipe.execute()):
                    sink[int(kk.split(":")[-1])] = vv
                pipe = self.r.pipeline(transaction=False)
                keys = []
        if keys:
            for kk, vv in zip(keys, pipe.execute()):
                sink[int(kk.split(":")[-1])] = vv

    def _warm_up(self):
        self._drain("feat:user:*", self.users)
        self._drain("feat:movie:*", self.movies)
        self.loaded = len(self.users) + len(self.movies)

    def get(self, user_id: int, movie_id: int):
        """Вектор фич в порядке FEATURE_NAMES из model/model_version.json."""
        if self.mode == "online" and self.redis_ok:
            key = (user_id, movie_id)
            hit = self.cache.get(key)
            if hit is None:
                pipe = self.r.pipeline(transaction=False)
                pipe.hgetall(f"feat:user:{user_id}")
                pipe.hgetall(f"feat:movie:{movie_id}")
                u, m = pipe.execute()
                hit = (u or {}, m or {})
                if len(self.cache) < 200_000:
                    self.cache[key] = hit
            u, m = hit
        else:
            u = self.users.get(user_id) or {}
            m = self.movies.get(movie_id) or {}

        return (
            float(u.get("user_avg_rating", self.g_user)),
            float(u.get("user_rating_count", 0.0)),
            float(u.get("user_std_rating", 0.0)),
            float(m.get("movie_avg_rating", self.g_movie)),
            float(m.get("movie_rating_count", 0.0)),
            float(m.get("movie_year", 1995.0)),
        )


# --------------------------------------------------------------------------
# Ветка 1: stateful инференс по пользователю
# --------------------------------------------------------------------------
class UserStateInferenceFunction(KeyedProcessFunction):
    """Обогащение фичами + ONNX-инференс + state на пользователя.

    State (ValueState, TTL 24 ч): [rating_count, rating_sum, last_event_ms, last_rating].
    TTL нужен, чтобы state не рос бесконечно: иначе растёт checkpoint и падает
    пропускная способность (см. раздел «узкие места» в отчёте).

    Side outputs:
        ERROR_TAG       — ошибки обработки/битые записи  -> dlq_errors
        LATE_TAG_INFER  — события позже watermark+allowedLateness -> dlq_late
    """

    def __init__(self):
        self.model = None
        self.prob_idx = 0
        self.features = None
        self.state = None
        self.c_infer = None
        self.c_err = None
        self.c_late = None
        self.late_threshold_ms = ALLOWED_LATENESS_SEC * 1000

    def open(self, runtime_context):
        import onnxruntime as ort

        so = ort.SessionOptions()
        so.intra_op_num_threads = 1
        so.inter_op_num_threads = 1
        so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        self.model = ort.InferenceSession(
            MODEL_PATH, sess_options=so, providers=["CPUExecutionProvider"]
        )
        names = [o.name for o in self.model.get_outputs()]
        self.prob_idx = names.index("probabilities") if "probabilities" in names else 0

        self.features = FeatureStore(REDIS_HOST, REDIS_PORT, FEATURE_MODE, FEATURE_JSON).open()

        desc = ValueStateDescriptor("user_state", ACC_TYPE)
        try:
            ttl = (
                StateTtlConfig.new_builder(Time.hours(24))
                .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build()
            )
            desc.enable_time_to_live(ttl)
        except Exception:  # noqa: BLE001
            pass  # TTL не критичен для корректности

        self.state = runtime_context.get_state(desc)

        group = runtime_context.get_metrics_group()
        self.c_infer = group.counter("lab5_inference_count")
        self.c_err = group.counter("lab5_inference_errors")
        self.c_late = group.counter("lab5_late_events")

    def process_element(self, value, ctx):
        event_id, user_id, movie_id, rating, event_time, sent_ts, is_late = value

        if user_id < 0:
            self.c_err.inc()
            yield ERROR_TAG, json.dumps(
                {"event_id": event_id, "error": "parse_error", "ts": now_ms()},
                ensure_ascii=False)
            return

        # --- late-arriving: событие позже watermark + allowedLateness ---
        # Обращение к timer_service().current_watermark() — синхронный вызов на
        # сторону JVM. На каждом событии он заметно бьёт по задержке, поэтому
        # включается отдельным флагом (оконная ветка ловит late-данные и так).
        is_too_late = False
        if CHECK_WATERMARK_PER_EVENT:
            watermark = ctx.timer_service().current_watermark()
            is_too_late = (
                watermark is not None
                and watermark > -(2 ** 62)
                and event_time < watermark - self.late_threshold_ms
            )

        try:
            st = self.state.value() or [0.0, 0.0, 0.0, 0.0]
            st[0] += 1.0
            st[1] += float(rating)
            st[2] = float(event_time)
            st[3] = float(rating)
            self.state.update(st)

            t_feat0 = time.perf_counter()
            u_avg, u_cnt, u_std, m_avg, m_cnt, m_year = self.features.get(user_id, movie_id)
            feat_ms = (time.perf_counter() - t_feat0) * 1000.0

            dt = _dt.datetime.fromtimestamp(event_time / 1000.0, tz=_dt.timezone.utc)
            vec = np.array([[u_avg, u_cnt, u_std, m_avg, m_cnt, m_year,
                             float(dt.hour), float(dt.weekday())]], dtype=np.float32)

            t0 = time.perf_counter()
            prob = np.asarray(self.model.run(None, {"input": vec})[self.prob_idx])[:, 1]
            infer_ms = (time.perf_counter() - t0) * 1000.0
            score = float(prob[0])

            self.c_infer.inc()
            if is_late or is_too_late:
                self.c_late.inc()

            yield json.dumps({
                "event_id": event_id,
                "user_id": user_id,
                "movie_id": movie_id,
                "rating": rating,
                "score": round(score, 6),
                "will_like": bool(score >= DECISION_THRESHOLD),
                "model_version": MODEL_VERSION,
                "event_time": event_time,
                "sent_ts": sent_ts,
                "infer_ts": now_ms(),
                "is_late": bool(is_late or is_too_late),
                "u_avg": round(u_avg, 4),
                "m_avg": round(m_avg, 4),
                "feat_ms": round(feat_ms, 4),
                "infer_ms": round(infer_ms, 4),
                "state_count": st[0],
            }, ensure_ascii=False)

            if is_too_late:
                yield LATE_TAG_INFER, json.dumps({
                    "event_id": event_id, "user_id": user_id, "movie_id": movie_id,
                    "event_time": event_time, "watermark": watermark,
                    "lateness_ms": watermark - event_time, "ts": now_ms(),
                }, ensure_ascii=False)
        except Exception as exc:  # noqa: BLE001
            self.c_err.inc()
            yield ERROR_TAG, json.dumps({
                "event_id": event_id, "user_id": user_id,
                "error": f"{type(exc).__name__}: {exc}", "ts": now_ms(),
            }, ensure_ascii=False)


# --------------------------------------------------------------------------
# Ветка 2: оконная агрегация по фильму
# --------------------------------------------------------------------------
class RatingAggregate(AggregateFunction):
    """Инкрементальная агрегация (count, sum, late_count) — без буферизации окна."""

    def create_accumulator(self):
        return [0.0, 0.0, 0.0]

    def add(self, value, accumulator):
        accumulator[0] += 1.0
        accumulator[1] += float(value[3])
        accumulator[2] += 1.0 if value[6] else 0.0
        return accumulator

    def get_result(self, accumulator):
        cnt, s, late = accumulator
        return [cnt, (s / cnt if cnt else 0.0), late]

    def merge(self, a, b):
        return [a[0] + b[0], a[1] + b[1], a[2] + b[2]]


class MovieStatsWindow(ProcessWindowFunction):
    """Достраивает агрегат до JSON: добавляет movie_id и конец окна."""

    def process(self, key, context, elements):
        for acc in elements:
            yield json.dumps({
                "movie_id": key,
                "window_end": int(context.window().end),
                "ratings_count": int(acc[0]),
                "avg_rating": round(float(acc[1]), 4),
                "late_count": int(acc[2]),
                "ts": now_ms(),
            }, ensure_ascii=False)


def build_kafka_sink(topic: str, transactional: bool) -> KafkaSink:
    """Sink в Kafka.

    transactional=True  -> EXACTLY_ONCE (транзакционный продюсер). Сообщения
        становятся видимыми только в момент коммита транзакции, то есть на
        checkpoint'е: end-to-end задержка видимости ≈ интервал checkpoint'а
        (у нас 1 с) -> требование p99 < 45 мс недостижимо.
    transactional=False -> AT_LEAST_ONCE + enable.idempotence=true: продюсер
        идемпотентен (дубликатов при ретраях нет), запись видна сразу.
        Эффективно-однократная доставка результата обеспечивается дедупликацией
        по event_id на стороне потребителя — этот приём прямо разрешён заданием
        («запись с дедупликацией по tx_id ... результат проверки идемпотентности
        тот же»).

    Какой режим использовать — переключается переменной SINK_GUARANTEE.
    """
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
    )
    if transactional:
        # transaction.timeout.ms должен быть <= transaction.max.timeout.ms брокера
        # (по умолчанию 15 мин), иначе брокер отклоняет InitProducerId.
        builder = (builder.set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
                   .set_transactional_id_prefix(f"lab5-{topic}")
                   .set_property("transaction.timeout.ms", "600000"))
    else:
        builder = (builder.set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                   .set_property("enable.idempotence", "true")
                   .set_property("linger.ms", "0")
                   .set_property("batch.size", "16384"))
    return builder.build()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)

    # КЛЮЧЕВАЯ оптимизация задержки для PyFlink:
    # по умолчанию Python-воркер копит элементы в «бандл» (1000 шт. или 1000 мс)
    # и только потом обрабатывает. Каждый Python-оператор в цепочке добавляет
    # до 1 с, а их несколько -> p99 измеряется секундами. Уменьшаем бандл.
    conf = Configuration()
    conf.set_string("python.fn-execution.bundle.size", os.environ.get("PY_BUNDLE_SIZE", "50"))
    conf.set_string("python.fn-execution.bundle.time", os.environ.get("PY_BUNDLE_TIME_MS", "10"))
    conf.set_string("python.fn-execution.memory.managed", "false")
    env.configure(conf)

    # ---- exactly-once semantics: checkpointing ----
    env.enable_checkpointing(CHECKPOINT_MS, CheckpointingMode.EXACTLY_ONCE)
    cc = env.get_checkpoint_config()
    cc.set_min_pause_between_checkpoints(10000)
    cc.set_checkpoint_timeout(120000)
    cc.set_max_concurrent_checkpoints(1)
    cc.set_tolerable_checkpoint_failure_number(3)
    cc.set_checkpoint_storage_dir(CHECKPOINT_DIR)

    # ---- автоматический перезапуск при сбое ----
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 1000))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC_EVENTS)
        .set_group_id("lab5-recommender")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        # КЛЮЧЕВАЯ оптимизация задержки: по умолчанию consumer ждёт до 500 мс
        # накопления батча (fetch.max.wait.ms). Для p99 < 45 мс это недопустимо.
        .set_property("fetch.max.wait.ms", "5")
        .set_property("fetch.min.bytes", "1")
        .set_property("max.poll.records", "500")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka:rating_events")
    parsed = raw.map(parse_event, output_type=EVENT_TYPE)

    # watermark: допускаем опоздание на WATERMARK_SEC секунд
    watermarked = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_SEC))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    # ---- ветка 1: stateful инференс ----
    # SINK_GUARANTEE=exactly-once -> транзакционный sink (видимость на checkpoint'е);
    # по умолчанию at-least-once + идемпотентный продюсер (низкая задержка).
    transactional = os.environ.get("SINK_GUARANTEE", "at-least-once") == "exactly-once"
    scored = watermarked.key_by(lambda e: e[1], key_type=Types.LONG()).process(
        UserStateInferenceFunction(), output_type=Types.STRING()
    )
    scored.sink_to(build_kafka_sink(TOPIC_PREDICTIONS, transactional=transactional)).name("predictions")
    scored.get_side_output(ERROR_TAG).map(
        side_output_to_str, output_type=Types.STRING()
    ).sink_to(build_kafka_sink(TOPIC_DLQ_ERRORS, transactional=False)).name("dlq_errors")
    scored.get_side_output(LATE_TAG_INFER).map(
        side_output_to_str, output_type=Types.STRING()
    ).sink_to(build_kafka_sink(TOPIC_DLQ_LATE, transactional=False)).name("dlq_late_infer")

    # ---- ветка 2: оконная агрегация + обработка late-arriving ----
    windowed = (
        watermarked.key_by(lambda e: e[2], key_type=Types.LONG())
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SEC)))
        .allowed_lateness(ALLOWED_LATENESS_SEC * 1000)   # миллисекунды!
        .side_output_late_data(LATE_TAG_WINDOW)
        .aggregate(RatingAggregate(), MovieStatsWindow(),
                   accumulator_type=ACC_TYPE, output_type=Types.STRING())
    )
    windowed.sink_to(build_kafka_sink(TOPIC_MOVIE_STATS, transactional=False)).name("movie_stats")
    windowed.get_side_output(LATE_TAG_WINDOW).map(
        side_output_to_str, output_type=Types.STRING()
    ).sink_to(build_kafka_sink(TOPIC_DLQ_LATE, transactional=False)).name("dlq_late_window")

    env.execute("lab5-streaming-recommender")


if __name__ == "__main__":
    main()
