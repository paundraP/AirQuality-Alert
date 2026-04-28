import json
import time
import os
import threading
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

# === KONFIGURASI ===
KAFKA_BROKER = 'kafka-broker:29092'      # ✅ internal docker network
TOPICS = ['airquality-api', 'airquality-rss']
HDFS_URL = 'http://namenode:9870'        # ✅ internal docker network
HDFS_USER = 'root'

HDFS_PATHS = {
    'airquality-api': '/data/airquality/api',
    'airquality-rss': '/data/airquality/rss',
}

LOCAL_PATHS = {
    'airquality-api': '/app/dashboard/data/api',   # ✅ path di dalam container
    'airquality-rss': '/app/dashboard/data/rss',
}

BUFFER_TIME = 120

for path in LOCAL_PATHS.values():
    os.makedirs(path, exist_ok=True)

# HDFS client
client = None
try:
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    print("[HDFS] Client berhasil dibuat")
except Exception as e:
    print(f"[HDFS WARNING] Client gagal: {e}")

buffer_data = {
    'airquality-api': [],
    'airquality-rss': [],
}
buffer_lock = threading.Lock()


def save_buffer():
    while True:
        time.sleep(BUFFER_TIME)

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
                    with client.write(hdfs_file, encoding='utf-8') as writer:
                        json.dump(data_to_save, writer, indent=4)
                    print(f"[HDFS] Uploaded: {hdfs_file}")
                except Exception as e:
                    print(f"[HDFS ERROR] {topic}: {e}")
            else:
                print(f"[HDFS SKIP] Client tidak tersedia untuk {topic}")


def consume_topic(topic_name):
    print(f"--- [START] Consumer: {topic_name} ---")

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        group_id='airquality-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

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
