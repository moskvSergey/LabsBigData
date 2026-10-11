# GPU-трек (Часть 6)

## Какой трек выполнен

**Трек А (CPU): Polars/DuckDB против pandas.**

## Причина выбора

На машине отсутствует NVIDIA GPU (или нет возможности развернуть 
CUDA-окружение). Трек B (cuDF for Apache Spark) недоступен: 
для него нужны NVIDIA GPU с драйвером R525+ и CUDA 12.9 или 13.1, 
контейнер с `--gpus all` и Linux-хост. На Windows/macOS без NVIDIA 
этот трек технически невозможен.

## Что сделано в Треке А

Сравнение трёх инструментов на одной задаче (join ratings + movies, 
фильтр по жанру Action, агрегация по title):

| Инструмент | 100 МБ | Полный датасет |
|---|---|---|
| pandas | 1.39 сек | 11.51 сек |
| Polars | 0.26 сек | 2.36 сек |
| DuckDB | 0.10 сек | 0.82 сек |

**Ускорение относительно pandas на полном датасете:**
- Polars: 4.88×
- DuckDB: 14.05×

## Вывод

DuckDB в 14 раз быстрее pandas за счёт векторизованного SQL 
и работы с Parquet напрямую. Polars в 5 раз быстрее за счёт 
многопоточности и Arrow-массивов. GPU на нашем объёме не окупается: 
запрос выполняется меньше 30 секунд, а инициализация GPU-executor 
занимает 2–3 секунды.

## Команды запуска (если GPU когда-нибудь появится)

```bash
# Только при наличии NVIDIA GPU
docker build -t cudf-spark-lab -f gpu/Dockerfile .
docker run --gpus all -it --rm -v "$PWD":/workspace -w /workspace cudf-spark-lab bash

spark-submit \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=0.25 \
  app.py