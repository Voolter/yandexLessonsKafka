FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir faust-streaming faust-streaming-rocksdb

COPY stream_processor.py .
COPY models.py .