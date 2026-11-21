# ЛР-6, Часть 6. Образ с автоматическими проверками.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN pip install --no-cache-dir pyyaml pandas pyarrow

COPY resource_management/ /app/resource_management/
COPY tests/ /app/tests/
COPY kubernetes/ /app/kubernetes/
COPY monitoring/ /app/monitoring/
COPY cicd/ /app/cicd/
COPY dvc/ /app/dvc/
COPY config/ /app/config/
COPY load_generator/ /app/load_generator/
COPY README.md /app/README.md

CMD ["python", "tests/check_requirements.py"]
