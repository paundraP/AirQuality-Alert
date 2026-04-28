import feedparser
import json
import time
import hashlib
from datetime import datetime
from kafka import KafkaProducer

# === KONFIGURASI ===
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'airquality-rss'

RSS_SOURCES = {
    "tempo": "https://www.tempo.co/rss/nasional",
    "kompas": "https://news.google.com/rss/search?q=source:kompas.com+polusi+udara",
    "detik": "https://news.google.com/rss/search?q=source:detik.com+polusi+udara"
}

# Menyimpan ID yang sudah dikirim agar tidak duplikat
sent_articles = set()

def fetch_rss_news():
    print("--- Memulai Pooling RSS Berita (Anti-Duplicate & Hash) ---")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        for sumber, url in RSS_SOURCES.items():
            feed = feedparser.parse(url)
            for entry in feed.entries[:8]: # Ambil 8 berita per sumber
                # Generate Hash 8 karakter (Request Paundra)
                article_id = hashlib.md5(entry.link.encode()).hexdigest()[:8]
                
                if article_id in sent_articles:
                    print(f"[SKIP] Duplikat: {article_id}")
                    continue
                
                payload = {
                    "id": article_id,
                    "judul": entry.title,
                    "link": entry.link,
                    "ringkasan": entry.get('summary', entry.title)[:200],
                    "waktu_terbit": entry.get('published', datetime.now().isoformat()),
                    "sumber": sumber,
                    "timestamp_ingested": datetime.now().isoformat()
                }
                
                producer.send(TOPIC_NAME, value=payload)
                sent_articles.add(article_id)
                print(f"[SENT RSS] {sumber.upper()} | {article_id} | {entry.title[:50]}...")
                time.sleep(0.3)

        producer.flush()
        print("\nRSS Pooling Selesai!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_rss_news()