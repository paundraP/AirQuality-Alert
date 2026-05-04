# AirQuality-Alert

Pemantauan Air Quality Index (AQI) kota-kota besar di jawa timur dan mengirimkan peringatan saat kualitas udara memburuk.

## How to start:

1. Hadoop
`docker compose -f docker-compose-hadoop.yml up -d`

2. Kafka
`docker compose -f docker-compose-kafka.yml up -d`

Cek: docker ps → harus ada zookeeper, kafka-broker, kafka-ui

3. Jalanin Init Script

`bash scripts/init-kafka-topics.sh`

4. Install requirements yang dibutuhin

`pip install kafka-python requests feedparser pyspark flask`

5. Jalanin Kafka untuk collect API dan RSS serta dimasukin ke hdfs

`bash scripts/run-producers-and-wait-hdfs.sh`

6. Jalanin notebook Spark untuk menghasilkan `dashboard/data/spark_results.json`

`bash scripts/run-spark-analysis.sh`

### Jalanin Semua Sekaligus (untuk A5/Dashboard)

Kalau pengen end-to-end dari setup sampai data JSON siap dipakai dashboard:

```bash
# Pastikan compose Hadoop & Kafka sudah jalan duluan, lalu:
bash scripts/run-all.sh
```

Script ini memanggil `init-kafka-topics.sh` -> `run-producers-and-wait-hdfs.sh` -> `run-spark-analysis.sh` secara berurutan. Hasil akhir di `dashboard/data/spark_results.json` (untuk endpoint `/api/spark`) plus folder `dashboard/data/api/` & `dashboard/data/rss/` (untuk `/api/live` & `/api/news`).

Flag opsional untuk skip step:

```bash
SKIP_INIT=1       bash scripts/run-all.sh   # lewati setup topics+HDFS
SKIP_INGESTION=1  bash scripts/run-all.sh   # lewati producer
SKIP_SPARK=1      bash scripts/run-all.sh   # lewati Spark
```

## Arsitektur Sistem

```
[AQICN API / OpenAQ]     [RSS Feed Lingkungan]
          |                       |
          v                       v
  [kafka/producer_api.py]  [kafka/producer_rss.py]
          |                       |
          v                       v
  [Topic: airquality-api]  [Topic: airquality-rss]
                  \\           /
                   v         v
            [kafka/consumer_to_hdfs.py]
                  /           \\
                 v             v
          [HDFS Storage]   [dashboard/data/ (local copy)]
                 |                     |
                 v                     v
        [spark/analysis.ipynb]   [dashboard/app.py]
                 |                     |
                 v                     v
        [spark_results.json]  ——>  [localhost:5000]
```


### Port Mapping

| Port | Service              |
| ---- | -------------------- |
| 9870 | HDFS NameNode Web UI |
| 9000 | HDFS RPC             |
| 8088 | YARN ResourceManager |
| 9092 | Kafka Broker         |
| 8080 | Kafka UI             |
| 5000 | Flask Dashboard      |
