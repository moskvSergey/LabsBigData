#!/bin/bash
# Создание топиков Kafka для ЛР-5.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

echo "Ожидание брокера ${BOOTSTRAP}..."
for i in $(seq 1 60); do
  if "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

create() {
  local topic="$1" parts="$2"
  "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists --topic "${topic}" \
    --partitions "${parts}" --replication-factor 1
}

# 3 партиции у входного топика -> параллелизм Flink до 3
create rating_events  3
create predictions    3
create movie_stats    1
create dlq_errors     1
create dlq_late       1

echo "Топики:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
