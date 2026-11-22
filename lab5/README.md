# Лабораторная работа 5. Проектирование систем потоковой обработки и данных

**Вариант:** последняя цифра зачётки — **8**
**Уникальное требование к задержке:** p99 < **45 мс** (обязательно к выполнению)
**Датасет:** MovieLens ml-32m (из ЛР-1, срез `ratings_100mb.parquet` — 4 млн оценок)
**Бизнес-сценарий:** Рекомендательная система онлайн-кинотеатра (продолжение ЛР-1…ЛР-4)

---

## 1. Что сделано

Потоковый пайплайн реального времени: события-оценки из Kafka обогащаются фичами из
Feature Store (Redis) и проходят online-инференс ONNX-модели прямо в потоке.

```
                  ┌──────────────────────────────────────────────────────────────┐
   Kafka          │  parse (JSON → event-time)                                   │
   rating_events  │                                                              │
   (3 партиции)   └───┬──────────────────────────────────────────────┬───────────┘
                      │ keyBy(user_id)                   keyBy(movie_id)│
                      ▼                                              ▼
   ┌────────────────────────────────────┐   ┌────────────────────────────────────────┐
   │ UserStateInferenceFunction         │   │ Tumbling 1 мин, watermark 5 с          │
   │  • ValueState (TTL 24 ч)           │   │  • allowedLateness 5 мин               │
   │  • фичи из Redis + ONNX-инференс   │   │  • side output → dlq_late              │
   │  • ошибки → dlq_errors             │   │  • инкрементальный aggregate           │
   └───────────────┬────────────────────┘   └───────────────┬────────────────────────┘
                   ▼                                        ▼
            Kafka predictions                        Kafka movie_stats
                   │
                   ▼
        monitor (перцентили, PSI, /metrics)  →  Prometheus  →  Grafana
```

**Стек:** Flink 1.20.1 (PyFlink DataStream API) · Kafka 3.9 (KRaft) · Redis 7.2 ·
ONNX Runtime · MLflow · Prometheus · Grafana · Docker Compose.

---

## 2. Запуск

```bash
cd lab5
docker compose up -d --build          # поднимается весь стенд
docker compose logs -f monitor        # здесь печатается блок ## METRICS ##
docker compose run --rm tests         # автоматические проверки
```

Проверка модели отдельно (вне Docker):

```bash
python model/train_export.py          # обучение + экспорт ONNX + версия в MLflow
python feature_store/redis/load_features.py --host localhost
```

**Адреса:**

| Сервис | URL |
|---|---|
| Flink Web UI | http://localhost:8081 |
| Grafana (дашборд «ЛР-5») | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Model Server (FastAPI) | http://localhost:8010/docs |
| Метрики монитора | http://localhost:8001/metrics |

> Порт 8000 на хосте занят другим проектом, поэтому model server опубликован на 8010.

---

## 3. Проектные решения (Часть 1)

### 3.1. Micro-batch или true streaming

| Критерий | Micro-batch (Spark Structured Streaming) | True streaming (Flink) |
|---|---|---|
| Минимальная задержка | ограничена интервалом триггера (десятки–сотни мс) | единицы мс на событие |
| Стоимость | дешевле: батчи эффективнее по CPU | дороже: событие обрабатывается поштучно |
| Гарантии | exactly-once через checkpoint + идемпотентный sink | то же + нативный event-time и side outputs |
| Late-arriving | watermark + `dropDuplicates` | watermark + `allowedLateness` + side output |

**Выбор: true streaming на Flink.** Требование варианта 8 (p99 < 45 мс) отсекает
micro-batch: даже при `Trigger.ProcessingTime(0)` задержка батча складывается из
планирования JVM-задач и обычно превышает 45 мс. Плата — выше стоимость CPU на событие.

### 3.2. Матрица требований и решений

| Требование | Решение | Обоснование |
|---|---|---|
| Задержка p99 < 45 мс | Flink true streaming + ONNX в процессе + tuning Kafka/PyFlink | см. раздел 5 |
| Гарантия обработки всех событий | checkpointing (EXACTLY_ONCE) + идемпотентный продюсер + дедуп по `event_id` | см. раздел 4.3 |
| Late-arriving / out-of-order | watermark 5 с + `allowedLateness` 5 мин + side output в `dlq_late` | окно успевает «дозакрыться» |
| Интеграция с ML | Feature Store (Redis) + ONNX Runtime, версия модели в MLflow | инференс в потоке, без сетевого хопа |
| Мониторинг и алертинг | Flink Prometheus reporter + Prometheus + Grafana + PSI-дрейф | 3 панели дашборда, 6 алертов |
| Обработка ошибок | retry, DLQ (`dlq_errors`), restart-strategy | битые записи не роняют поток |

### 3.3. Допустимая задержка и стратегия late-arriving

- **Допустимая задержка:** 5 минут (`allowedLateness`), watermark — 5 секунд.
- **Стратегия:** `watermark + allowedLateness + side outputs` (а не «просто дропать»).
  Событие, опоздавшее больше чем на 5 минут, не теряется: оно уходит в `dlq_late`,
  откуда его можно дообработать пакетно.

---

## 4. Реализация (Часть 2–4)

### 4.1. Модель

`model/train_export.py`:

- временной сплит 80/20 (обучение на прошлом, проверка на будущем);
- фичи: `user_avg_rating`, `user_rating_count`, `user_std_rating`, `movie_avg_rating`,
  `movie_rating_count`, `movie_year`, `hour`, `day_of_week`;
- статистики пользователей/фильмов считаются **только на train** (иначе утечка);
- холодный стартер закрывается глобальными средними из `feat:global`.

| Модель | AUC | F1 (порог подобран на train) |
|---|---|---|
| **LogisticRegression** (выбрана) | **0.6591** | **0.4870** |
| XGBoost | 0.6609 | 0.5664 |

Для потока выбрана логистическая регрессия: разница в AUC ~0.002, а latency инференса
на порядок ниже — это критично для бюджета 45 мс.

ONNX-экспорт: opset 17 (совместим с onnxruntime 1.20 в образе Flink),
проверка эквивалентности sklearn ↔ ONNX: `max|diff| = 8.9e-08`.

**Инференс ONNX:** p50 = **0.03 мс**, p99 = **0.18 мс** (одиночный вызов, CPU).
В потоке (с учётом вызова из Python-воркера) p50 = **0.10 мс**, p99 = **4.09 мс**.

Версия модели фиксируется в MLflow (`lab5_streaming_recommender`) и в
`models/model_version.json`.

### 4.2. Feature Store

`feature_store/redis/load_features.py` кладёт в Redis 50 629 ключей:

```
feat:user:<userId>   → user_avg_rating, user_rating_count, user_std_rating
feat:movie:<movieId> → movie_avg_rating, movie_rating_count, movie_year
feat:global          → средние для холодного стартера
```

В задании два режима (`FEATURE_MODE`):

- `warm` (по умолчанию) — воркер выгружает фичи из Redis в память при старте:
  **0.02 мс** на событие;
- `online` — `HGETALL` в Redis на каждое событие: добавляет ~0.2–0.5 мс.

Если Redis недоступен, пайплайн не падает, а читает снапшот `work/feature_store.json`.

### 4.3. Exactly-once

Три уровня:

1. **Checkpointing** (`EXACTLY_ONCE`, каждые 30 с, таймаут 120 с, min-pause 10 с) —
   состояние восстанавливается, offset'ы Kafka коммитятся вместе с checkpoint'ом.
2. **Идемпотентный продюсер** (`enable.idempotence=true`) — дубликатов при ретраях нет.
3. **Дедупликация по `event_id`** на стороне потребителя — приём, прямо разрешённый
   заданием («запись с дедупликацией по `tx_id` … результат проверки идемпотентности тот же»).

Почему не транзакционный sink по умолчанию: `DeliveryGuarantee.EXACTLY_ONCE` делает
запись видимой только в момент коммита транзакции, то есть на checkpoint'е. При интервале
1 с это добавляет ~1–2 с к end-to-end задержке и делает требование 45 мс невыполнимым
арифметически. Транзакционный режим сохранён и включается переменной
`SINK_GUARANTEE=exactly-once`.

**Проверка:** на прогоне 800/800 и 1200/1200 событий — `DUPLICATES=0`, `ERRORS=0`.

### 4.4. Обработка late-arriving данных

- watermark `for_bounded_out_of_orderness(5 с)`;
- окно 1 минута + `allowedLateness(300 000 мс)`;
- `side_output_late_data` → топик `dlq_late`;
- плюс опциональная проверка `current_watermark()` в stateful-функции
  (`CHECK_WATERMARK_PER_EVENT=1`).

**Проверка:** 5 % событий отправлялись с `event_time` на 6 минут в прошлом —
все они дошли до пайплайна и были учтены (`LATE_DATA_HANDLED=True`).

### 4.5. State management

`UserStateInferenceFunction` — `KeyedProcessFunction` с `ValueState`
`[rating_count, rating_sum, last_event_ms, last_rating]` и TTL 24 часа.
TTL обязателен: без него state растёт неограниченно и checkpoint раздувается.

---

## 5. Задержка и анализ узких мест (Часть 5–7)

### 5.1. Что измеряем

`monitoring/monitor.py` для каждого `event_id` считает:

- **end-to-end** — от отправки в топик до появления предикта в `predictions`;
- **inference** — от отправки до штампа `infer_ts` внутри Flink (без sink'а и потребителя).

### 5.2. Итоговый прогон (800 событий, 40 соб/с, 5 % late)

```
REQUIRED_LATENCY=45
MEASURED_P50_LATENCY=234.42
MEASURED_P95_LATENCY=1284.83
MEASURED_P99_LATENCY=2721.67
PIPELINE_P99_LATENCY=2705.90
INFERENCE_P99_LATENCY=4.09
SLA_OK=False
LATE_DATA_HANDLED=True
LATE_EVENTS=60
PREDICTIONS=799
DUPLICATES=0
ERRORS=0
THROUGHPUT_EPS=28.8
CHECKPOINT_DURATION_SEC=0.03
DATA_DRIFT_SCORE=0.2437

> **Про воспроизводимость.** Замер зависит от того, успевает ли конвейер за подачей.
> Пропускная способность PyFlink-конвейера ~25–30 событий/с, поэтому при подаче
> 40 событий/с копится очередь и p99 растёт до секунд. Цифры выше получены при
> подаче 40 событий/с после перезапуска стенда (очередь пуста). Ниже порога
> насыщения (15 событий/с) разброс между прогонами минимален, а картина та же:
> сам инференс — единицы миллисекунд, всё остальное — накладные расходы
> Python DataStream API.
```

### 5.3. Разбор по этапам

Замеры с разделением этапов (серия из 40 событий):

| Конфигурация | source + обработка | sink + потребитель |
|---|---|---|
| по умолчанию | ~4000 мс | ~50 мс |
| `bundle.size=50, bundle.time=10 мс` | **~370 мс** | ~50 мс |

**Узкое место №1 — «бандлы» Python-воркера PyFlink.** По умолчанию воркер копит
1000 элементов **или 1000 мс** и только потом обрабатывает. В цепочке четыре
Python-оператора (`parse` → timestamp assigner → key selector → `KeyedProcessFunction`),
то есть до 4 с постоянной добавки к задержке. Настройки читаются **на клиенте** при
построении графа задания, поэтому их нужно задавать и в контейнере `flink-submit`, и
передавать через `env.configure(Configuration())` (в `FLINK_PROPERTIES` jobmanager'а
их недостаточно).

**Узкое место №2 — `fetch.max.wait.ms` в Kafka-консьюмере.** По умолчанию consumer
ждёт до 500 мс накопления батча. Установлено `fetch.max.wait.ms=5`, `fetch.min.bytes=1`.

**Узкое место №3 — видимость транзакционного sink'а.** См. раздел 4.3.

**Узкое место №4 — пропускная способность Python DataStream API.** С уменьшением
бандла растёт число gRPC-вызовов на событие, и устойчивая пропускная способность
падает до ~30 событий/с. Это и есть причина, по которой при подаче 40 соб/с
наблюдается очередь и p99 растёт до ~20 с: задержка обслуживания × 1/(1−ρ).

### 5.4. Вывод по требованию p99 < 45 мс

**Требование не выполнено на текущем стеке.** Честная формулировка результата:

- **инференс** укладывается в бюджет с огромным запасом: p99 = **4.09 мс** в потоке
  (бюджет сценария — 100 мс, уникальное требование — 45 мс);
- **end-to-end через PyFlink Python DataStream API** — нет: после тюнинга p50 = 234 мс,
  p99 = 2.7 с; при подаче выше ~30 событий/с добавляется очередь и p99 растёт до секунд.

Причина не в архитектуре (Flink + event-time + ONNX выбраны верно), а в
**Python DataStream API**: каждый Python-оператор — это gRPC-хоп в отдельный
Python-воркер (Beam FnApi) с собственным буфером.

**Что нужно, чтобы уложиться в 45 мс** (рекомендации для production):

1. Перенести разбор, watermark и оконную агрегацию в **Flink SQL / Java-операторы** —
   тогда Python остаётся только на инференс (один хоп вместо четырёх).
2. Либо вынести инференс в **model server** и вызывать его асинхронно
   (`AsyncDataStream`), не блокируя поток.
3. Увеличить параллелизм источника (партиции топика ≥ параллелизма задачи).
4. Для варианта «micro-batch допустим» — Spark Structured Streaming + ONNX
   в `foreachBatch` (как в Части 4 задания), с оговоркой по бюджету задержки.

---

## 6. Мониторинг и алертинг (Часть 6)

- Flink отдаёт метрики в Prometheus (`metrics.reporter.prom`, порты 9250/9251);
- `monitoring/monitor.py` публикует бизнес-метрики на `:8001/metrics`:
  задержки p50/p95/p99, пропускная способность, late-события, ошибки,
  длительность checkpoint, PSI-дрейф, счётчики дубликатов;
- **PSI** считается по фиче `user_avg_rating` относительно offline-распределения
  (21 434 пользователя из фиче-стора). На прогоне PSI = **0.2455** > порога 0.2 →
  дрейф зафиксирован (на синтетическом потоке из среза датасета это ожидаемо);
- **Grafana**, дашборд `streaming.json`, три панели: состояние потока, качество
  данных, состояние state/checkpoint;
- **6 алертов** в `monitoring/prometheus/alerts.yml`: p99 > 45 мс, p95 > 30 мс,
  высокий lag в Kafka, рост ошибок, PSI > 0.2, падение/замедление checkpoint'ов.

---

## 7. Тестирование и валидация (Часть 8)

| Тест | Что проверяет |
|---|---|
| `tests/test_latency.py` | p99 из живого прогона + живой замер отправка→предикт |
| `tests/test_late_data.py` | late-arriving события доходят и учитываются |
| `tests/check_requirements.py` | сводная проверка всех требований + блок `## METRICS ##` |
| `tests/metrics.py` | печать фиксированных метрик для отчёта |

Генератор нагрузки (`load_generator/generator.py`) умеет режимы `normal`, `peak` (×2),
`late`, `burst`, пишет реальные оценки MovieLens и штампует `sent_ts` для замера.

---

## 8. Структура

```
lab5/
├── README.md
├── docker-compose.yml
├── requirements.txt
├── lab5.ipynb                     # основной ноутбук отчёта
├── model/train_export.py          # обучение + ONNX + MLflow
├── models/                        # recommender.onnx, model_version.json
├── flink/
│   ├── Dockerfile                 # PyFlink + Kafka-коннектор + Prometheus reporter
│   ├── requirements.txt
│   └── jobs/
│       ├── recommender_stream.py  # потоковое задание
│       └── cancel_running.py      # идемпотентность docker compose up
├── feature_store/redis/load_features.py
├── model_server/{Dockerfile,model.py}
├── load_generator/{Dockerfile,generator.py}
├── monitoring/
│   ├── monitor.py
│   ├── prometheus/{prometheus.yml,alerts.yml}
│   └── grafana/{dashboards/streaming.json,provisioning/...}
├── kafka/topics/init.sh
├── tests/{check_requirements.py,test_latency.py,test_late_data.py,metrics.py,common.py}
└── work/                          # feature_store.json, metrics.json (генерируется)
```

---

## 9. Итоги

**Работает и проверено на живом стенде:**

- потоковая обработка: окна 1 мин, watermark 5 с, `allowedLateness` 5 мин, side outputs;
- state management: keyed `ValueState` с TTL 24 ч;
- late-arriving данные: обработаны, посчитаны, не потеряны;
- exactly-once/идемпотентность: 0 дубликатов на 2000 отправленных событий;
- интеграция с ML: online-инференс ONNX, фичи из Redis, версия модели в MLflow;
- fault tolerance: checkpointing, restart-strategy, DLQ для ошибок;
- мониторинг: метрики, дашборд, 6 алертов, PSI-дрейф;
- весь стенд поднимается одной командой `docker compose up -d`.

**Не достигнуто:** end-to-end p99 < 45 мс. Причина и план исправления — раздел 5.4.
