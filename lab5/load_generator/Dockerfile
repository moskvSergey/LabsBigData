# Генератор нагрузки: пишет события-оценки MovieLens в Kafka.
FROM python:3.11-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir \
        kafka-python==2.2.15 \
        pandas==2.2.3 \
        pyarrow==18.1.0

WORKDIR /opt/load_generator
COPY generator.py /opt/load_generator/generator.py

CMD ["python", "generator.py"]
