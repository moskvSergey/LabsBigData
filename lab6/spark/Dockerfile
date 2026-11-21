# ЛР-6, Часть 1. Оптимизированный образ Spark.
# Многоступенчатая сборка: tgz скачивается и распаковывается в builder,
# в финальный образ копируется только распакованный дистрибутив без
# apt-кэша, архивов и документации.
#
# Замер размеров (docker images):
#   naive  (ubuntu:22.04 + openjdk-11 + spark tgz + apt-кэш): ~1.15 GB
#   optimized (eclipse-temurin:17-jre-jammy, multi-stage):   ~0.50 GB
#
# JVM в контейнере: UseContainerSupport + MaxRAMPercentage — иначе JVM
# видит память хоста, а не cgroup-лимит пода, и получает OOMKilled.

# ---------- Stage 1: builder ----------
FROM eclipse-temurin:17-jdk-jammy AS builder
ARG SPARK_VERSION=3.3.0
ARG HADOOP_PROFILE=hadoop3
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSL -o /tmp/spark.tgz \
       "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
    && mkdir -p /opt/spark \
    && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
    && rm -f /tmp/spark.tgz \
    && rm -rf /opt/spark/examples /opt/spark/data /opt/spark/kubernetes /opt/spark/python/docs \
    && rm -rf /var/lib/apt/lists/*

# ---------- Stage 2: runtime ----------
FROM eclipse-temurin:17-jre-jammy AS runtime
LABEL org.opencontainers.image.title="lab6-spark" \
      org.opencontainers.image.description="ЛР-6 optimized Spark image (variant 8)"

ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYSPARK_PYTHON=python3 \
    # Контейнерная осведомлённость JVM: 75% лимита пода под heap, остальное — overhead
    JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 libsnappy1v5 libgomp1 zlib1g procps \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10002 spark

COPY --from=builder /opt/spark /opt/spark

# Непривилегированный пользователь: если под скомпрометируют, root в контейнере не дадут.
USER spark
WORKDIR /opt/spark/work-dir
