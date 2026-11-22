# Образ Flink для потокового пайплайна ЛР-5.
# Используется и для jobmanager, и для taskmanager (различаются командой запуска).
FROM flink:1.20.1-scala_2.12-java17

USER root

# --- Python для PyFlink ---
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev curl ca-certificates \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# --- Коннектор Kafka (нужен для KafkaSource/KafkaSink в PyFlink) ---
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# --- Репортер метрик в Prometheus (критерий "Мониторинг и алертинг") ---
RUN mkdir -p /opt/flink/plugins/metrics-prometheus \
    && curl -fsSL -o /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-1.20.1.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.20.1/flink-metrics-prometheus-1.20.1.jar

# --- Python-зависимости потокового задания ---
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/jobs
