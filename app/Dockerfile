FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka

COPY BatchMessageConsumer.py .
COPY SingleMessageConsumer.py .
COPY Producer.py .