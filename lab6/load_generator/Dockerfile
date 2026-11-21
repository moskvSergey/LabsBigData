# ЛР-6, Часть 1. Образ генератора нагрузки.
# Сборка из корня lab6 (context: .), чтобы забрать общий cost_model.py.
FROM python:3.11-slim AS runtime
LABEL org.opencontainers.image.title="lab6-load-generator"

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    COST_MODEL_CONFIG=/app/resource_management/cost_model.json
WORKDIR /app

RUN pip install --no-cache-dir "pandas>=2.0" "pyarrow>=14.0"

COPY load_generator/generator.py /app/generator.py
COPY resource_management/cost_model.py /app/cost_model.py
# cost_model.json читается по пути resource_management/cost_model.json
COPY resource_management/cost_model.json /app/resource_management/cost_model.json

CMD ["python", "generator.py", "--mode", "normal", "--events", "600"]
