import requests
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'airquality-api'

BASE_URL = "https://ispu.kemenlh.go.id/apimobile/v1"


def normalize_kota(nama):
    if not nama:
        return ""
    nama = nama.lower()
    nama = nama.replace("kota ", "").replace("kabupaten ", "")
    return nama.strip()


def get_stations_jatim_unique():
    url = f"{BASE_URL}/getStations"
    res = requests.get(url, timeout=10).json()

    daerah = []
    kota_terpakai = set()

    for s in res.get("rows", []):
        if s.get("provinsi") == "Jawa Timur":
            kota_raw = s.get("kota") or ""
            kota = normalize_kota(kota_raw)

            if kota in kota_terpakai:
                continue

            kota_terpakai.add(kota)
            daerah.append(s.get("id_stasiun"))

    return daerah


def get_data(station_id):
    url = f"{BASE_URL}/getDetail/stasiun/{station_id}"
    try:
        res = requests.get(url, timeout=10).json()

        if res.get('status', {}).get('statusCode') == 200 and res.get('rows'):
            return res['rows'][0]
    except Exception as e:
        print(f"[ERROR] {station_id}: {e}")

    return None


def fetch_and_produce():
    print("--- Memulai Ingestion API Jatim (Clean & Unique) ---")

    DAERAH_JATIM = get_stations_jatim_unique()
    print(f"Total kota unik: {len(DAERAH_JATIM)}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for station_id in DAERAH_JATIM:
            data = get_data(station_id)

            # 🔥 skip kalau data kosong atau AQI None
            if not data or data.get("val") is None:
                print(f"[SKIP] {station_id}: No valid AQI")
                continue

            raw_kota = data.get("kota") or station_id

            payload = {
                "id_stasiun": station_id,
                "kota": normalize_kota(raw_kota),
                "aqi": data.get("val"),
                "pm25": data.get("pm25") or data.get("a_pm25"),
                "timestamp": data.get("waktu"),
                "kategori": data.get("cat"),
                "sumber": "ispu_kemenlh"
            }

            producer.send(TOPIC_NAME, value=payload)

            # ✅ format log sesuai contoh kamu
            print(f"[SENT API] {station_id}: {payload['aqi']} ISPU ({payload['kategori']})")

            time.sleep(1)

        producer.flush()
        print("\nAPI Ingestion Selesai!")

    except Exception as e:
        print(f"[KAFKA ERROR] {e}")


if __name__ == "__main__":
    fetch_and_produce()