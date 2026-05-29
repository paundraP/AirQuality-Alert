import json
import os
import threading
import time
from datetime import datetime

from hdfs import InsecureClient
from kafka import KafkaConsumer

# === KONFIGURASI ===
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka-broker:29092')
TOPICS = ['airquality-api', 'airquality-rss']
HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
HDFS_USER = os.getenv('HDFS_USER', 'root')

HDFS_PATHS = {
    'airquality-api': '/data/airquality/api',
    'airquality-rss': '/data/airquality/rss',
}

LOCAL_PATHS = {
    'airquality-api': '/app/dashboard/data/api',   # ✅ path di dalam container
    'airquality-rss': '/app/dashboard/data/rss',
}

BUFFER_TIME = int(os.getenv('BUFFER_TIME', '120'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))

for path in LOCAL_PATHS.values():
    os.makedirs(path, exist_ok=True)

buffer_data = {
    'airquality-api': [],
    'airquality-rss': [],
}
buffer_lock = threading.Lock()


def get_hdfs_client():
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    client.status('/')
    return client


def save_buffer():
    while True:
        time.sleep(BUFFER_TIME)

        try:
            client = get_hdfs_client()
            print(f"[HDFS] Connected: {HDFS_URL}")
        except Exception as e:
            client = None
            print(f"[HDFS WARNING] Client gagal: {e}")

        for topic in TOPICS:
            with buffer_lock:
                if not buffer_data[topic]:
                    print(f"[SAVE] Buffer kosong untuk {topic}, skip")
                    continue

                data_to_save = buffer_data[topic].copy()
                buffer_data[topic].clear()

            print(f"--- [SAVE] {topic}: {len(data_to_save)} events ---")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp}.json"

            # === SAVE LOCAL ===
            local_file = os.path.join(LOCAL_PATHS[topic], filename)
            try:
                with open(local_file, 'w') as f:
                    json.dump(data_to_save, f, indent=4)
                print(f"[LOCAL] Saved: {local_file}")
            except Exception as e:
                print(f"[LOCAL ERROR] {topic}: {e}")

            # === SAVE HDFS ===
            if client:
                hdfs_file = f"{HDFS_PATHS[topic]}/{filename}"
                try:
                    client.makedirs(HDFS_PATHS[topic])
                    with client.write(hdfs_file, encoding='utf-8') as writer: # pyright: ignore[reportOptionalContextManager]
                        json.dump(data_to_save, writer, indent=4)
                    print(f"[HDFS] Uploaded: {hdfs_file}")
                except Exception as e:
                    print(f"[HDFS ERROR] {topic}: {e}")
            else:
                print(f"[HDFS SKIP] Client tidak tersedia untuk {topic}")


def consume_topic(topic_name):
    print(f"--- [START] Consumer: {topic_name} ---")
    consumer = None

    while consumer is None:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                group_id='airquality-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"[KAFKA] Connected to {KAFKA_BROKER} for {topic_name}")
        except Exception as e:
            print(f"[KAFKA WARNING] {topic_name}: {e}. Retry in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)

    for message in consumer:
        event = message.value
        event['topic_origin'] = topic_name
        event['ingested_at'] = datetime.now().isoformat()

        with buffer_lock:
            buffer_data[topic_name].append(event)

        print(f"[RECEIVED] {topic_name} | offset: {message.offset}")


if __name__ == "__main__":
    t1 = threading.Thread(target=consume_topic, args=('airquality-api',))
    t2 = threading.Thread(target=consume_topic, args=('airquality-rss',))
    t3 = threading.Thread(target=save_buffer, daemon=True)

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
