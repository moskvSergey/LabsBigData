#!/usr/bin/env bash
#
# ЛР-6, Часть 4, п.4. Автоматический откат деплоя.
#
# Запускается CI/CD-шагом `Rollback on failure`, когда метрики кандидата не
# прошли гейт (p99 > 100 мс, доля ошибок > 5%, PSI > 0.2, экономия < 40%).
#
# Почему скрипт, а не отдельный манифест: откат обязан быть идемпотентным и
# не конфликтовать по именам с production.yaml. Мы просто возвращаем прод на
# проверенный образ и гасим канареечный трек.
#
# Использование:
#   ./cicd/scripts/rollback.sh [namespace] [previous_image_tag]

set -euo pipefail

NAMESPACE="${1:-lab6-prod}"
PREVIOUS_TAG="${2:-lab6-model-server:1.0}"
DEPLOY="model-server"
CANARY="model-server-canary"

echo "== Откат: $DEPLOY -> $PREVIOUS_TAG (namespace $NAMESPACE) =="

echo "-- 1/4 возвращаем проверенный образ"
kubectl -n "$NAMESPACE" set image "deployment/$DEPLOY" "model-server=$PREVIOUS_TAG"

echo "-- 2/4 гасим канареечный трек (весь трафик на stable)"
kubectl -n "$NAMESPACE" scale "deployment/$CANARY" --replicas=0 || true

echo "-- 3/4 ждём готовности (maxUnavailable: 0 — без простоя)"
kubectl -n "$NAMESPACE" rollout status "deployment/$DEPLOY" --timeout=180s

echo "-- 4/4 smoke-тесты после отката"
python "$(dirname "$0")/smoke_test.py" --endpoint "http://$DEPLOY.$NAMESPACE" --wait 60

echo "== Откат завершён: прод на $PREVIOUS_TAG =="
echo "Если проблема была в данных — RESTORE TABLE ratings TO VERSION AS OF <n> (cicd/delta/ratings_delta.sql)"
