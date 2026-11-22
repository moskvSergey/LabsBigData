# Лабораторные работы по Big Data

Репозиторий с двумя лабораторными работами по курсу Big Data.

**Вариант:** последняя цифра зачётки — **8**
**Датасет:** MovieLens ml-32m (32 млн оценок фильмов, 1995–2023)
**Бизнес-сценарий:** рекомендательная система для онлайн-кинотеатра

## Состав

- `lab1.ipynb` — Лабораторная работа 1: Анализ Big Data-сценариев по 5V
- `lab2.ipynb` — Лабораторная работа 2: Сравнение инструментов обработки
- `gpu/README.md` — отчёт по GPU-треку (Часть 6 Лабы 2)
- `requirements.txt` — зависимости
- `.gitignore` — исключения (данные не коммитятся)

## Лабораторная работа 1: Анализ по 5V

Анализ датасета MovieLens под сценарий рекомендательной системы.

**Ключевые результаты:**

| Критерий | Результат |
|---|---|
| Volume | 0.77 ГБ CSV → 0.20 ГБ Parquet; хранение < 10 ₽/год |
| Velocity | 0.035 зап/сек среднее, пик 548× выше; нужен streaming |
| Variety | 93.6% числовых, < 1% текста |
| Veracity | 0 дубликатов, 0 аномалий, 5 проверок GE пройдены |
| Value | ROI положительный, окупаемость ~2 мес |

**Технологический стек:** S3 + Delta Lake + Spark + Kafka + MLflow + FastAPI + Airflow.

## Лабораторная работа 2: Сравнение инструментов

Задача: соединение ratings + movies по movieId, фильтр по жанру Action, 
агрегация по названию фильма.

**Ключевые результаты:**

| Инструмент | Полный датасет | Ускорение |
|---|---|---|
| Pandas | 11.51 сек | 1.0× |
| Spark | 6.71 сек | 1.7× |
| Polars | 2.36 сек | 4.9× |
| **DuckDB** | **0.82 сек** | **14.1×** |

**Что сделано:**
- Сравнение производительности Pandas / Dask / Spark / Polars / DuckDB
- Анализ физического плана Spark (BroadcastHashJoin + filter pushdown)
- Проверка skew по ключу `title` (умеренный, AQE справляется)
- 5 оптимизаций Spark с замерами (лучший эффект — broadcast hint, +12%)
- Трек А: Polars/DuckDB vs pandas на CPU

**Рекомендация:** для данных до 100 ГБ — DuckDB или Polars. 
Spark оправдан при объёме > 100 ГБ или требовании распределённой 
отказоустойчивости. GPU на нашем объёме не окупается.

## Воспроизведение

```bash
pip install -r requirements.txt

# Скачать датасет MovieLens ml-32m
# https://grouplens.org/datasets/movielens/
# Распаковать в папку ml-32m/ рядом с ноутбуками

jupyter notebook
```

# Лабораторная работа 3: Построение промышленных ETL/ELT пайплайнов

## Описание
Пайплайн для рекомендательной системы на базе MovieLens.
Вариант: последняя цифра зачетки — 8.
Сценарий: Ежечасное обновление рекомендаций (SLA < 45 минут).

## Стек технологий
- **Spark (PySpark)** — обработка данных
- **Prefect** — оркестрация
- **Great Expectations** — качество данных
- **DBT + DuckDB** — трансформации (ELT)
- **Parquet** — хранение (имитация Delta Lake)

## Как запустить
1. Установить зависимости: `pip install -r requirements.txt`
2. Открыть Jupyter Notebook (`lab3.ipynb`)
3. Запустить все ячейки сверху вниз (`Kernel -> Restart & Run All`)

## Метрики
- **Идемпотентность:** PASSED (0 дубликатов)
- **DLQ:** 10 битых записей перехвачено
- **Время пайплайна:** 3.14 сек
- **DBT тесты:** 6/6 PASSED



# Лабораторная работа 4: Интеграция ML-моделей в production

## Описание
Пайплайн рекомендательной системы на базе MovieLens.
Вариант: последняя цифра зачетки — 8.
Сценарий: Рекомендательная система.
Уникальное требование (8): Обработка временных паттернов (сезонность).

## Стек технологий
- **MLflow** (SQLite backend) — управление жизненным циклом моделей.
- **Scikit-Learn / XGBoost** — обучение моделей.
- **Custom Feature Store** — согласованность фич Offline/Online.
- **FastAPI** — инференс-сервис.
- **Evidently** — мониторинг Data Drift.
- **River** — онлайн-обучение.

## Метрики
- **Feature Match:** PASSED (Offline и Online фичи идентичны).
- **Latency (p95):** 17.86 мс (требование < 100 мс).
- **Online/Batch Difference:** 0.1028.
- **Unique Requirement Impact:** +0.0003.

## Как запустить
1. Установить зависимости: `pip install -r requirements.txt`
2. Открыть Jupyter Notebook (`lab4.ipynb`).
3. Запустить все ячейки сверху вниз (`Kernel -> Restart & Run All`).

---

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

# Лабораторная работа 6. DevOps для Big Data — развёртка, мониторинг и управление ресурсами

**Вариант:** последняя цифра зачётки — **8**
**Бизнес-сценарий:** Рекомендательная система (персонализированные рекомендации в реальном времени)
**Датасет:** MovieLens ml-32m (из ЛР-1), срез `ratings_100mb.parquet` — 4 млн оценок
**Уникальное требование по cost optimization:**
> **Снизить затраты на 40% через комбинацию on-demand и spot** (без ущерба для SLA)

**Требования сценария:** latency инференса < **100 мс** · SLA доступности **99.5%** · CTR +10%
**Стек:** Kubernetes (kind) · Spark Operator / Flink Operator · KEDA · Cluster Autoscaler ·
Docker (multi-stage) · Prometheus · Grafana · GitHub Actions · DVC · Delta Lake · FastAPI

---

## 1. Итоговые метрики (воспроизводятся командой `make test`)

```
## METRICS ##
REQUIRED_COST_SAVINGS=40
ACHIEVED_COST_SAVINGS=41.06
SYSTEM_AVAILABILITY=0.999990
COST_PER_JOB=4.0742
DATA_DRIFT_SCORE=0.0022
BASELINE_COST_PER_JOB=6.9120
COST_OPTIMIZATION_FACTOR=1.697
SPOT_FRACTION=0.75
REQUIRED_LATENCY_MS=100
MEASURED_P99_LATENCY_MS=4.884      # живой прогон: 800 событий, 100 соб/с
THROUGHPUT_EPS=100.87
SCHEMA_COMPLIANCE=1.0000
SLA_OK=True
COST_OK=True
```

| Требование | Цель | Факт | Статус |
|---|---|---|---|
| Экономия затрат (вариант 8) | ≥ 40% | **41.06%** | ✅ |
| Cost per job (оптимизированный) | < baseline | **$4.07** против $6.91 | ✅ |
| Cost optimization factor | > 1 | **1.70×** | ✅ |
| Доступность (SLA) | ≥ 99.5% | **99.999%** | ✅ |
| Latency инференса p99 | < 100 мс | **4.88 мс** (end-to-end) / **0.06 мс** (в процессе) | ✅ |
| Schema compliance | ≥ 0.99 | **1.00** | ✅ |
| Data drift (PSI) | < 0.2 | **0.0022** | ✅ |
| Автопроверки | 48 проверок | **48/48 пройдено** | ✅ |

![График снижения затрат](work/cost_reduction.png)

---

## 2. Запуск и проверка

### 2.1. Офлайн-цикл (без Docker и без кластера) — самый быстрый путь

```bash
cd lab6
pip install -r requirements.txt

make demo          # cost + график + валидация манифестов + данные + drift + все проверки
make metrics       # только блок ## METRICS ##
```

Отдельные шаги:

```bash
make cost          # cost-модель: baseline vs on-demand+spot
make chart         # work/cost_reduction.png
make manifests     # YAML/apiVersion/spot-toleration/GPU-профиль (offline)
make validate-data # качество данных (Great Expectations-совместимо)
make drift         # PSI data drift по срезу датасета
```

### 2.2. Демо-стенд целиком одной командой

```bash
docker compose up -d --build
docker compose run --rm tests        # ## METRICS ##
```

Нагрузочный тест: режим и размер задаются переменными окружения хоста
(`MODE`, `EVENTS`, `RATE`), потому что флаг `run -e` не переопределяет значения
из блока `environment` в compose-файле:

```bash
# bash
MODE=normal EVENTS=800 docker compose run --rm load-generator
# PowerShell
$env:MODE="peak"; $env:EVENTS="800"; docker compose run --rm load-generator
```

| Сервис | Адрес |
|---|---|
| Model Server (FastAPI) | http://localhost:8000/docs |
| Метрики Prometheus-формата | http://localhost:8000/metrics |
| Prometheus | http://localhost:9090 |
| Grafana (3 дашборда «ЛР-6») | http://localhost:3000 (admin/admin) |
| Redis (feature store) | localhost:6379 |

### 2.3. Kubernetes (kind)

```bash
pwsh -File scripts/deploy.ps1 -DryRun     # показать план
pwsh -File scripts/deploy.ps1             # создать кластер + применить манифесты
pwsh -File scripts/deploy.ps1 -Cleanup    # удалить кластер
```

Скрипт создаёт кластер с **двумя пулами узлов** (`on-demand` и `spot` с taint
`spot=true:NoSchedule`), собирает образ `lab6-model-server:1.0`, загружает его в kind
и применяет манифесты. GPU не нужен: GPU-профиль проверяется через
`kubectl apply --dry-run=client`.

---

## 3. Часть 1. Контейнеризация Big Data-приложений (2.0 балла)

### 3.1. Оптимизированные образы

| Компонент | Dockerfile | Приём оптимизации |
|---|---|---|
| Spark | `spark/Dockerfile` | многоступенчатая сборка: tgz качается и распаковывается в builder, в финальный образ идёт только дистрибутив |
| Model Server | `model_server/Dockerfile` | venv-зависимости из builder + `python:3.11-slim`, непривилегированный `appuser`, HEALTHCHECK |
| Load Generator | `load_generator/Dockerfile` | slim-база, `--no-cache-dir`, общий `cost_model.py` |
| Kafka / Redis / Grafana / Prometheus | официальные образы | `apache/kafka:3.9.0` (KRaft), `redis:7.2-alpine`, `grafana/grafana:11.1.0`, `prom/prometheus:v2.53.0` |

Размеры образов (замер `docker images`):

| Образ | «Наивная» сборка | Оптимизированная | Как достигнуто |
|---|---|---|---|
| Spark | ~1.15 GB (`ubuntu:22.04` + `openjdk-11-jdk` + tgz + apt-кэш) | **~0.50 GB** | multi-stage, `jre` вместо `jdk`, вырезаны `examples/`, `data/`, `kubernetes/`, `docs/`, чистка apt-кэша в том же слое |
| Model Server | ~230 MB (`python:3.11` + pip-кэш) | **294 MB** (замер `docker images`) | `python:3.11-slim`, venv из builder, `--no-cache-dir`, `PYTHONDONTWRITEBYTECODE` |
| Load Generator | ~600 MB | **653 MB** (pandas + pyarrow) | slim-база, `--no-cache-dir` |

### 3.2. Лимиты ресурсов

В `docker-compose.yml` у каждого сервиса заданы `limits` и `reservations`
(например, model-server: limits 2 CPU / 1 GB, reservations 0.5 CPU / 512 MB).
`reservations` — это тот «правильный размер инстанса», с которым HPA/KEDA считает
реальную утилизацию, а не номинальную.

JVM честно видит cgroup-лимит контейнера:

```dockerfile
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"
```

Без `UseContainerSupport` JVM берёт память **хоста**, а не пода, и получает `OOMKilled`
при первом же крупном shuffle. `MaxRAMPercentage=75.0` оставляет 25% на
`memoryOverhead` (off-heap, metaspace, native).

**Как контейнеризация решает проблемы Big Data:** фиксирует окружение (один и тот же
Spark везде), изолирует ресурсы (одна задача не съедает кластер), делает образ
артефактом версионирования и позволяет запускать одно и то же локально и в кластере.

---

## 4. Часть 2. Оркестрация с Kubernetes (2.5 балла)

Манифесты: `kubernetes/` — 8 файлов, 29 объектов Kubernetes.

| Что | Файл | Ключевые решения |
|---|---|---|
| Spark Operator + `SparkApplication` | `spark-operator/spark-operator.yaml`, `manifests/spark-app.yaml` | driver 2 CPU/4 GB на **on-demand**, executor 4 CPU/8 GB на **spot**, `dynamicAllocation` 2…20, restartPolicy OnFailure×3 |
| Flink Operator + `FlinkDeployment` | `manifests/flink-app.yaml` | JobManager 1 реплика / TaskManager 3 реплики, RocksDB state, checkpoint 30 c EXACTLY_ONCE, `upgradeMode: savepoint` |
| StatefulSet + PVC | `manifests/flink-app.yaml` | `flink-taskmanager` c `volumeClaimTemplates` (10 Gi, `standard`) и anti-affinity |
| Сетевые решения для shuffle | `manifests/spark-app.yaml` | `podAntiAffinity` по `kubernetes.io/hostname` — executor'ы по разным узлам |
| Autoscaling (KEDA + Cluster Autoscaler) | `keda/scaledobject.yaml` | ScaledObject по Kafka-lag (2…20 реплик), ConfigMap для cluster-autoscaler |
| GPU-профиль | `spark-operator/spark-operator.yaml` | `nvidia.com/gpu: 1`, `spark.plugins=com.nvidia.spark.SQLPlugin`, `spark.task.resource.gpu.amount=0.25` |
| Прод/канарейка/откат | `deploy/*.yaml`, `cicd/scripts/rollback.sh` | HPA 2…10, PDB `minAvailable: 80%`, rolling update `maxUnavailable: 0` |

**Почему Operators, а не «голые» Deployment'ы:** оператор знает предметную область —
сам поднимает драйвер, чистит executor'ы, восстанавливает Flink из savepoint'а и
перезапускает задачу при вытеснении узла. Руками это превращается в набор скриптов,
которые ломаются при каждом обновлении.

**Affinity и tolerations.** Driver обязан быть на on-demand: его падение стоит дороже
всей экономии на spot, а перезапуск драйвера обнуляет progress. Executor'ы
stateless — их вытеснение лечится пересчётом stage из shuffle-файлов, поэтому они
живут на spot с toleration:

```yaml
tolerations:
  - key: "spot"
    operator: "Equal"
    value: "true"
    effect: "NoSchedule"
```

---

## 5. Часть 3. Управление ресурсами и cost optimization (2.0 балла)

### 5.1. Стратегия варианта 8: on-demand + spot

| Слой | Пул | Почему |
|---|---|---|
| Spark driver, Flink JobManager | **on-demand** | потеря драйвера = потеря всей задачи |
| Spark executor (75% executor-времени) | **spot** | stateless, вытеснение лечится пересчётом stage |
| Model server: 6 реплик | **spot** + anti-affinity | 6 реплик в разных зонах, вытеснение снимает 1/6 мощности |
| Model server: 1 «якорная» реплика | **on-demand** | страховка SLA на случай массового вытеснения |
| Батч-задачи | spot через KEDA | автоскейл по Kafka-lag, нет данных — нет расходов |

### 5.2. Cost-модель (`resource_management/cost_model.json` + `cost_model.py`)

Исходные данные: задача ETL `ratings_etl_hourly`, 8 executor-часов на прогон,
инстанс `c5.4xlarge` (16 vCPU / 32 GB): **$0.768/час on-demand**, **$0.2688/час spot**
(типичная цена spot ≈ 35% от on-demand).

```python
baseline_cost  = 8.0 ч * $0.768                     = $6.912   # 100% on-demand
optimized_cost = (8.0 * 1.05) ч смеси 75% spot       = $4.074   # fault-tolerance overhead +5%
savings        = (6.912 - 4.074) / 6.912 * 100       = 41.06%   # нужно >= 40%
factor         = 6.912 / 4.074                       = 1.70x
```

**Откуда берётся overhead +5%:** при вытеснении spot-узла упавший stage пересчитывается
из checkpoint'ов. Overhead заложен **сознательно** — стратегия «spot без fault tolerance»
в отчёте не принимается: экономия была бы фиктивной.

За месяц (720 прогонов): **$4 976.64 → $2 933.45**, экономия **$2 043.19**.

### 5.3. Остальные механики Части 3

| Механика | Реализация |
|---|---|
| YARN Capacity Scheduler | `resource_management/yarn/capacity-scheduler.xml`: очереди `etl` 60%, `ml` 30%, `default` 10%, `maximum-capacity` не даёт вытеснить ML-очередь батчем |
| Dynamic allocation | `resource_management/spark/dynamic-allocation.conf`: 2…20 executor'ов, `shuffleTracking.enabled=true` (не нужен External Shuffle Service), AQE для укрупнения партиций |
| KEDA autoscaling | `kubernetes/keda/scaledobject.yaml`: `lagThreshold=1000`, `minReplicaCount=2`, cooldown 120 c, scale-down стабилизация 300 c |
| Cluster Autoscaler | ConfigMap `cluster-autoscaler-config`: `scaleDownUnneededTime=30m`, `scaleDownUtilizationThreshold=0.5` |
| Правильные размеры инстансов | cpu-bound → `c5.4xlarge` ($0.768/ч), memory-bound → `r5.4xlarge` ($1.152/ч) с 128 GB |
| Память | `spark.memory.fraction=0.6`, `storageFraction=0.5`, `memoryOverhead=1g` |
| GPU-профиль | манифест + расчёт окупаемости: `utilization = 45 / (45 + 2.5) = 94.7%`, `break_even = QUERY_SECONDS >= 30` |

GPU-профиль на этом стенде **не запускается**: у машины нет NVIDIA-карты, поэтому
манифест проверяется локально (`kubectl apply --dry-run=client`), а прогон
выполняется на NVIDIA-стенде или в облаке. Это прямо разрешено заданием
(«баллы при фолбэке те же»). Важная деталь: **без `spark.plugins` ускорения не
будет вовсе** — Spark выполнит тот же код на CPU-пути, а карта останется
простаивать; и **один GPU на executor** — несколько GPU на executor Spark не
поддерживает.

### 5.4. График снижения затрат

`make chart` → `work/cost_reduction.png` (baseline $6.91 → optimized $4.07, −41.1%).

---

## 6. Часть 4. CI/CD для данных и моделей (1.5 балла)

### 6.1. Data CI/CD — три гейта (`cicd/.github/workflows/ci.yml`)

| Job | Что делает | Почему гейт именно такой |
|---|---|---|
| `validate` | проверка схемы/диапазонов/уникальности (`tests/validate_data.py`, GX-совместимо) + offline-валидация манифестов | битые данные ломают пайплайн позже и дороже |
| `test` | `tests/check_requirements.py` + явный **cost-гейт**: `cost_savings_pct >= 40` и `availability >= 0.995` | уникальное требование варианта 8 становится блокирующим, а не «пожеланием» |
| `deploy` | canary → smoke-тесты → окно наблюдения → метрики → promote **или** автооткат | перейти на новую версию без риска для 100% трафика |

### 6.2. Data Version Control (DVC)

`dvc/dvc.yaml` описывает стадии `validate` и `cost-report`; `dvc/config` — локальный
remote `../dvc-storage` (в проде заменяется на `s3://lab6-dvc/movielens`).
В git попадают только `.dvc`-указатели: датасет не раздувает репозиторий,
но любая версия данных воспроизводима.

### 6.3. Delta Lake: версионирование таблиц

`cicd/delta/ratings_delta.sql`: `MERGE` вместо `INSERT` (повторный прогон
идемпотентен), `DESCRIBE HISTORY`, time travel (`VERSION AS OF 10`),
**`RESTORE TABLE ratings TO VERSION AS OF 10`** — это и есть «откат деплоя» для
данных, `OPTIMIZE ... ZORDER BY` + `VACUUM RETAIN 168 HOURS` — стоимость хранения.

### 6.4. Безопасный деплой

```
staging  ->  smoke-тесты  ->  canary (1 под из 7 ≈ 14%, задание: 5%)
         ->  check_metrics.py --baseline-endpoint ...  ->  PROMOTE | ROLLBACK
```

* `cicd/scripts/smoke_test.py` — `/health`, `/predict`, `/predict/batch` (score в [0,1]);
* `cicd/scripts/check_metrics.py` — p99, доля ошибок, PSI, schema compliance, экономия;
  печатает `DEPLOY_DECISION=PROMOTE|ROLLBACK` и возвращает код 1 при провале;
* `cicd/scripts/rollback.sh` — идемпотентный откат: `kubectl set image` на
  проверенный тег + `scale canary --replicas=0` + rollout status + smoke.

**Метрики сравнения версий:** p99 latency, доля ошибок (success ratio), PSI,
schema compliance, cost per job.

---

## 7. Часть 5. Мониторинг и алертинг (1.5 балла)

### 7.1. Три уровня наблюдаемости

| Уровень | Источник | Что видно |
|---|---|---|
| Инфраструктура | `kubernetes_sd_configs` (роль `node`, порт 9100) + kube-state-metrics | CPU/memory/network подов, число узлов Ready, spot-вытеснения |
| Приложение | `model-server:8000/metrics`, Spark PrometheusServlet, Flink reporter | p50/p95/p99 latency, RPS, ошибки, shuffle spill, checkpoint duration |
| Данные | PSI в `/metrics` (принимается через `POST /metrics/ingest`) | data drift, schema compliance, объём DLQ |

Проверено на живом стенде: Prometheus скрейпит `model-server:8000` со статусом `up`
и видит бизнес-метрики — `lab6_data_drift_psi=0.0022`,
`lab6_cost_savings_pct=41.06`, `lab6_cost_per_job_usd=4.0742`,
`lab6_schema_compliance_ratio=1.0`, `lab6_dlq_records_total=0`.
Целевые job'ы кластера (`spark-driver`, `flink-*`, `kube-state-metrics`) в демо-стенде
помечены `down` — они поднимаются только вместе с Kubernetes (`scripts/deploy.ps1`).

### 7.2. Алерты (`monitoring/prometheus/alerts.yml`) — 12 правил в 4 группах

| Группа | Примеры | Реакция |
|---|---|---|
| SLA | `HighInferenceLatencyP99` (>100 мс, critical), `ServiceUnavailable`, `HighErrorRate` | поднять реплики/HPA, проверить spot-вытеснение |
| Cost | `CostSavingsBelowTarget` (<40%), `CostPerJobHigh` (>$4.5), `SpotNodeEvictions` | поднять долю spot, проверить dynamic allocation |
| Данные | `HighDataDrift` (PSI>0.2, critical), `SchemaComplianceLow` | переобучить модель на свежем срезе, проверить DLQ |
| Инфраструктура | `HighShuffleSpill` (>1 ГБ/10 мин), `ContainerMemoryNearLimit` (>90%), `CheckpointDurationHigh` | поднять `shuffle.partitions`, разобраться с OOMKilled, ускорить checkpoint |

**Стратегия реагирования:** critical в рабочие часы → немедленно (дежурный инженер +
автооткат в CI/CD); warning → тикет и разбор на недельном ревью. У каждого алерта
есть `runbook`-метка и описание «что делать», а не только «что случилось».

### 7.3. Дашборды Grafana

| Дашборд | Панели |
|---|---|
| Панель 1 — инфраструктура (`infrastructure.json`) | CPU (факт/лимит), memory working set vs limit, network rx/tx, узлы Ready |
| Панель 2 — приложение (`application.json`) | перцентили latency с порогами 45/100 мс, RPS и ошибки, SLA-доступность, shuffle spill + checkpoint, cost per job |
| Панель 3 — качество данных (`data.json`) | PSI-дрейф с порогом 0.2, schema compliance, DLQ за час, достигнутая экономия против цели 40% |

### 7.4. Проверка, что дрейф вообще детектируется

`tests/compute_drift.py` считает PSI по реальному срезу (20 000 оценок, 9 958
пользователей): два окна одного распределения дают **PSI = 0.0022** (`OK`).
Чтобы показать, что алерт не «мёртвый», в скрипте есть флаг инжекции дрейфа:

```bash
python tests/compute_drift.py                      # PSI = 0.0022 -> OK
python tests/compute_drift.py --inject-drift 0.8   # PSI = 4.4872 -> CRITICAL
```

Во втором прогоне продовое окно сдвинуто к нижней границе шкалы — PSI превышает
порог 0.2, и правило `HighDataDrift` переходит в `firing`.

Заодно проверено, что дрейф **не ложноотрицателен**: узкая выборка (500 строк от
одного пользователя) давала PSI 0.49, а сравнение оценок с выходом модели — 15.3.
Это ровно те две ошибки, из-за которых drift-мониторинг обычно и не работает:
несмещённая выборка и сопоставимые шкалы.

### 7.5. Интеграция мониторинга с CI/CD

`check_metrics.py` читает те же метрики, что видит Grafana, и превращает их в решение
о деплое. Плюс бизнес-метрики прогона (cost per job, время, drift) фиксируются в
артефакте `lab6-metrics` и в `work/*.json` — их можно приложить к отчёту.

---

## 8. Часть 6. Тестирование и валидация

### 8.1. Генератор нагрузки (`load_generator/generator.py`)

Пишет реальные оценки MovieLens в model server, штампует `event_time`,
считает перцентили и PSI. Режимы:

| Режим | Нагрузка | Что проверяет |
|---|---|---|
| `normal` | 100 соб/с | базовое соответствие SLA |
| `peak` | 200 соб/с (2× от нормы) | запас по пропускной способности |
| `fail` | normal + вытеснение узла (1/6 реплик недоступна `recovery_seconds`) | SLA при отказе, отсутствие потерь |
| `late` | 5% событий с `event_time` на 6 минут в прошлом | обработку late-arriving данных |

### 8.2. Результаты живого прогона (стенд `docker compose`, 800 событий на режим)

| Режим | События | Ошибки | p50, мс | p99, мс | Пропускная способность | SLA |
|---|---|---|---|---|---|---|
| `normal` (100 соб/с) | 800 | 0 | 2.26 | **4.88** | 100.9 соб/с | ✅ |
| `peak` (200 соб/с) | 800 | 0 | 2.60 | **6.23** | 205.8 соб/с | ✅ |
| `fail` (вытеснение узла) | 797 ok / 3 fail | 0.38% | 2.30 | **7.72** | 100.6 соб/с | ✅ |
| `late` (5% late-arriving) | 800 | 0 | 2.30 | **5.87** | 101.0 соб/с | ✅ |

* `late`: 40 событий из 800 пришли с `event_time` на 6 минут в прошлом и были
  обработаны, а не потеряны (`late_events=40`);
* `fail`: 3 недоступных запроса из 800 при вытеснении узла — это 0.38% потерь при
  пороге 1%; остальные обслужены за счёт репликации и retry;
* **инференс внутри процесса**: p99 = **0.058 мс** (из `/metrics`), то есть бюджет
  100 мс покрыт с запасом более чем в 1000 раз; end-to-end p99 = 4.9–7.7 мс —
  это накладные расходы HTTP и Python-клиента.

### 8.3. Автоматические проверки

```bash
python tests/check_requirements.py   # 48 проверок по всем частям + ## METRICS ##
python tests/test_cost.py            # уникальное требование: экономия >= 40%
python tests/test_sla.py             # доступность >= 99.5%, p99 < 100 мс
python tests/validate_manifests.py   # YAML, spot-toleration, GPU-профиль
python tests/validate_data.py        # качество данных (GX-совместимо)
python tests/compute_drift.py        # PSI data drift
```

Результат: **48 проверок, 0 провалов** (`work/check_requirements.json`).

### 8.4. Затраты до и после оптимизации

| Показатель | Baseline (100% on-demand) | Optimized (75% spot + dynamic allocation) | Изменение |
|---|---|---|---|
| Cost per job | $6.912 | **$4.074** | −41.1% |
| За месяц (720 прогонов) | $4 976.64 | **$2 933.45** | **−$2 043.19** |
| Cost optimization factor | 1.00× | **1.70×** | +70% |
| Доступность | 99.9% (1 узел, без репликации) | **99.999%** (6 реплик + on-demand якорь) | +0.099 п.п. |

### 8.5. Вывод о готовности к production

**Готово:**
* кластер описывается кодом (29 объектов, операторы Spark/Flink, автоскейл KEDA + HPA + Cluster Autoscaler);
* уникальное требование варианта 8 выполнено с запасом (**41.06% ≥ 40%**), при этом SLA не
  нарушен — механика «spot + checkpoint/retry + репликация + on-demand якорь»;
* наблюдаемость на трёх уровнях, 12 алертов, 3 дашборда;
* CI/CD с тремя гейтами, canary и автоматическим откатом;
* 48 автопроверок, воспроизводимые метрики.

**Ограничения (честно):**
* цифры cost-модели — расчётные по прайсу `c5.4xlarge`; фактический счёт зависит от
  реального процента вытеснений, который на учебном стенде не воспроизводится;
* GPU-профиль не запускается: нет NVIDIA-карты, проверяется манифестом (разрешено заданием);
* Kubernetes-стенд локальный (kind, один «spot»-узел), поэтому реальное поведение
  Cluster Autoscaler и KEDA проверено конфигурационно, а не под нагрузкой.

---

## 9. Структура репозитория

```
lab6/
├── README.md
├── Makefile                          # make demo / up / load-test / test
├── docker-compose.yml                # демо-стенд одной командой
├── requirements.txt
├── lab6.ipynb                        # ноутбук отчёта (исполнен)
├── kubernetes/
│   ├── kind-cluster.yaml             # 2 пула узлов: on-demand и spot
│   ├── spark-operator/spark-operator.yaml   # оператор + GPU-профиль
│   ├── manifests/spark-app.yaml      # SparkApplication (driver on-demand, executor spot)
│   ├── manifests/flink-app.yaml      # Flink Operator, FlinkDeployment, StatefulSet
│   ├── keda/scaledobject.yaml        # KEDA + Cluster Autoscaler + secret
│   └── deploy/{staging,canary,production}.yaml
├── spark/Dockerfile                  # multi-stage образ Spark
├── model_server/{Dockerfile,app.py,requirements.txt}
├── load_generator/{Dockerfile,generator.py}
├── resource_management/
│   ├── cost_model.py, cost_model.json, make_cost_chart.py
│   ├── yarn/capacity-scheduler.xml
│   └── spark/dynamic-allocation.conf
├── monitoring/
│   ├── prometheus/{prometheus.yml,alerts.yml}
│   └── grafana/{dashboards/{infrastructure,application,data}.json, provisioning/...}
├── cicd/
│   ├── .github/workflows/{ci.yml,deploy.yml}
│   ├── scripts/{smoke_test.py,check_metrics.py,rollback.sh}
│   └── delta/ratings_delta.sql
├── dvc/{dvc.yaml,config}
├── config/great_expectations/suites/ratings_suite.json
├── data/sample_ratings.csv           # 20 000 реальных оценок из ЛР-1
├── tests/                            # check_requirements, test_cost, test_sla, metrics,
│                                     # validate_manifests, validate_data, compute_drift, common
├── scripts/deploy.ps1                # kind: создать/применить/удалить
├── scripts/make_notebook.py          # генератор lab6.ipynb
└── work/                             # генерируемые отчёты и график
```
