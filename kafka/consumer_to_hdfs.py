import json
import time
import os
import threading
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

# === KONFIGURASI ===
KAFKA_BROKER = 'localhost:9092'
TOPICS = ['airquality-api', 'airquality-rss']
HDFS_URL = 'http://localhost:9870'
HDFS_PATH = '/user/dina/airquality/data'
LOCAL_PATH = 'dashboard/data'
BUFFER_TIME = 120 

os.makedirs(LOCAL_PATH, exist_ok=True)

try:
    client = InsecureClient(HDFS_URL, user='dina')
except Exception as e:
    print(f"Warning HDFS Client: {e}")

buffer_data = []
buffer_lock = threading.Lock()

def save_buffer():
    while True:
        time.sleep(BUFFER_TIME)
        global buffer_data
        with buffer_lock:
            if not buffer_data:
                continue
            
            print(f"--- [SAVE] Mengolah {len(buffer_data)} event dalam buffer ---")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp}.json"
            local_file = os.path.join(LOCAL_PATH, filename)
            hdfs_file = f"{HDFS_PATH}/{filename}"
            
            try:
                with open(local_file, 'w') as f:
                    json.dump(buffer_data, f, indent=4)
                print(f"[LOCAL] Saved: {local_file}")
            except Exception as e:
                print(f"[LOCAL ERROR] {e}")

            try:
                client.makedirs(HDFS_PATH)
                with client.write(hdfs_file, encoding='utf-8') as writer:
                    json.dump(buffer_data, writer)
                print(f"[HDFS] Uploaded: {hdfs_file}")
            except Exception as e:
                print(f"[HDFS ERROR] {e}. Pastikan HDFS nyala!")

            buffer_data = []

def consume_topic(topic_name):
    print(f"--- [START] Consumer untuk Topic: {topic_name} ---")
    # PERBAIKAN: Pakai value_deserializer, bukan serializer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        event = message.value
        event['topic_origin'] = topic_name
        event['ingested_at'] = datetime.now().isoformat()
        
        with buffer_lock:
            buffer_data.append(event)
        print(f"[RECEIVED] Data from {topic_name}")

if __name__ == "__main__":
    t1 = threading.Thread(target=consume_topic, args=('airquality-api',))
    t2 = threading.Thread(target=consume_topic, args=('airquality-rss',))
    t3 = threading.Thread(target=save_buffer, daemon=True)

    t1.start()
    t2.start()
    t3.start()
