# ML Model Server: FastAPI + ONNX Runtime.
FROM python:3.11-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir \
        fastapi==0.115.6 \
        uvicorn==0.34.0 \
        onnxruntime==1.20.1 \
        numpy==1.26.4 \
        prometheus-client==0.21.1

WORKDIR /opt/model_server
COPY model.py /opt/model_server/model.py

EXPOSE 8000
CMD ["uvicorn", "model:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
