FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python \
    hdfs \
    feedparser \
    requests \
    certifi

COPY . .

CMD ["python3", "consumer_to_hdfs.py"]
