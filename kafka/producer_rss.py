import feedparser
import json
import time
import hashlib
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'airquality-rss'

RSS_SOURCES = {
    # Tambah User-Agent langsung di URL tidak bisa, jadi pakai feedparser agent
    "tempo":  "https://www.tempo.co/rss/nasional",
    "kompas": "https://news.google.com/rss/search?q=polusi+udara+kompas&hl=id&gl=ID&ceid=ID:id",
    "detik":  "https://news.google.com/rss/search?q=polusi+udara+detik&hl=id&gl=ID&ceid=ID:id",
}

sent_articles = set()

def fetch_rss_news():
    print("--- Memulai Pooling RSS Berita (Anti-Duplicate & Hash) ---")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for sumber, url in RSS_SOURCES.items():
            # ✅ FIX 1: Set User-Agent agar tidak diblokir
            feedparser.USER_AGENT = (
                "Mozilla/5.0 (compatible; NewsAggregator/1.0; +https://yourproject.com)"
            )
            feed = feedparser.parse(url)

            # ✅ FIX 2: Debug log — tampilkan jumlah entry yang ditemukan
            print(f"[DEBUG] {sumber.upper()} → {len(feed.entries)} entries ditemukan | "
                  f"status: {getattr(feed, 'status', 'N/A')} | "
                  f"bozo: {feed.bozo}")

            if feed.bozo:
                print(f"  └─ bozo_exception: {feed.bozo_exception}")

            if not feed.entries:
                print(f"  └─ [WARN] Feed kosong, skip sumber: {sumber}")
                continue

            for entry in feed.entries[:8]:
                article_id = hashlib.md5(entry.link.encode()).hexdigest()[:8]

                if article_id in sent_articles:
                    print(f"  [SKIP] Duplikat: {article_id}")
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
                print(f"  [SENT RSS] {sumber.upper()} | {article_id} | {entry.title[:50]}...")
                time.sleep(0.3)

        producer.flush()
        print("\nRSS Pooling Selesai!")

    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    fetch_rss_news()
