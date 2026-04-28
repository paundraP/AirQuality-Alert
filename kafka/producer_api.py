import requests
import json
import time
from kafka import KafkaProducer

API_TOKEN = "4bdec7a67818fbbb2dec75c14b32227f6fc9b96b"
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'airquality-api'

DAERAH_JATIM = [
    "Bangkalan", "Banyuwangi", "Blitar", "Bojonegoro", "Bondowoso", "Gresik", 
    "Jember", "Jombang", "Kediri", "Lamongan", "Lumajang", "Madiun", "Magetan", 
    "Malang", "Mojokerto", "Nganjuk", "Ngawi", "Pacitan", "Pamekasan", "Pasuruan", 
    "Ponorogo", "Probolinggo", "Sampang", "Sidoarjo", "Situbondo", "Sumenep", 
    "Trenggalek", "Tuban", "Tulungagung", "Batu", "Surabaya"
]

def get_data(target, token):
    url = f"https://api.waqi.info/feed/{target}/?token={token}"
    res = requests.get(url).json()
    return res if res['status'] == 'ok' else None

def fetch_and_produce():
    print("--- Memulai Ingestion API Jatim (Final Mode) ---")
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    # Ambil data Surabaya sebagai fallback/referensi utama Jatim
    ref_data = get_data("surabaya", API_TOKEN)

    for kota in DAERAH_JATIM:
        data = get_data(kota, API_TOKEN)
        
        # Kalau kota tersebut gak ada sensor, pake data Surabaya (Reference)
        if not data:
            data = ref_data
            status_text = "[FALLBACK]"
        else:
            status_text = "[SENT API]"

        if data:
            content = data['data']
            payload = {
                "kota": kota.lower(),
                "aqi": content.get('aqi'),
                "pm25": content.get('iaqi', {}).get('pm25', {}).get('v'),
                "timestamp": content.get('time', {}).get('iso'),
                "sumber": "aqicn_jatim"
            }
            producer.send(TOPIC_NAME, value=payload)
            print(f"{status_text} {kota}: {payload['aqi']} AQI")
        
        time.sleep(0.5)
    producer.flush()
    print("\nAPI Ingestion Selesai!")

if __name__ == "__main__":
    fetch_and_produce()
