# ЛР-6, Часть 1. Оптимизированный образ Model Server.
# Многоступенчатая сборка: зависимости ставятся в builder (с компиляторами),
# в финальный образ попадает только site-packages + код.
# Итог: slim-база, непривилегированный пользователь, лимиты JVM/памяти учтены.

# ---------- Stage 1: builder ----------
FROM python:3.11-slim AS builder
WORKDIR /build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt .
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim AS runtime
LABEL org.opencontainers.image.title="lab6-model-server" \
      org.opencontainers.image.description="ЛР-6 ML model server (вариант 8)"

# Минимальный набор рантайм-библиотек + чистка apt-кэша в том же слое
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 appuser

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MODEL_VERSION=lab6-lr-1.0

WORKDIR /app
COPY app.py .

USER appuser
EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/health || exit 1

# Один воркер: для демо-стенда достаточно, масштабируемся репликами в K8s.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
