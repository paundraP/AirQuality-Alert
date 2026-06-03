# 00_setup.md — Cara Menjalankan Pipeline Lakehouse

## Prasyarat

Stack Docker dari ETS harus sudah berjalan:
```bash
docker-compose up -d
docker-compose ps   # pastikan semua container status "Up"
```

---

## Menjalankan Pipeline Lakehouse

Semua script dijalankan di dalam container `scheduler` karena container itu sudah punya PySpark, delta-spark, dan akses ke HDFS.

### Step 1 — Masuk ke container

```bash
docker exec -it scheduler bash
```

### Step 2 — Set env var (sekali saja)

```bash
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell"
export HDFS_API_PATH="hdfs://namenode:9000/data/airquality/api/"
export HDFS_RSS_PATH="hdfs://namenode:9000/data/airquality/rss/"
```

### Step 3 — Jalankan Bronze

```bash
cd /app
python3 lakehouse/01_bronze.py
```

Output yang diharapkan:
```
[BRONZE-API] Ditemukan 60 baris dari HDFS
[BRONZE-API] Selesai. 60 baris → ./lakehouse_data/bronze/airquality_api
[BRONZE-RSS] Ditemukan 16 baris dari HDFS
[BRONZE-RSS] Selesai. 16 baris → ./lakehouse_data/bronze/airquality_rss
```

### Step 4 — Jalankan Silver

```bash
python3 lakehouse/02_silver.py
```

Output yang diharapkan:
```
[SILVER] Jumlah baris Bronze (sebelum cleaning): 60
[T4] Setelah filter null: 60 baris
[T5] Setelah filter anomali AQI: 60 baris
[T6] Setelah deduplikasi: 60 baris
[SILVER] Selesai. 60 baris → ./lakehouse_data/silver/airquality
```

### Step 5 — Jalankan Gold + Time Travel

```bash
python3 lakehouse/03_gold.py
```

Output yang diharapkan:
```
[G1] Selesai → ./lakehouse_data/gold/aqi_category_dist
[G2] Selesai → ./lakehouse_data/gold/aqi_hourly
[G3] Selesai → ./lakehouse_data/gold/aqi_ranking
[G4] Selesai → ./lakehouse_data/gold/aqi_trend
[G5] Selesai → ./lakehouse_data/gold/aqi_alert_hours
=== DEMONSTRASI TIME TRAVEL ===
...
```

---

## Verifikasi Output

Cek folder yang terbentuk:
```bash
ls -la lakehouse_data/bronze/airquality_api/
ls -la lakehouse_data/silver/airquality/
ls -la lakehouse_data/gold/
```

Setiap folder Delta table berisi:
- File `*.parquet` — data aktual
- Folder `_delta_log/` — transaction log (ini yang memungkinkan Time Travel)

---

## Dependencies

| Package | Versi | Keterangan |
|---------|-------|------------|
| pyspark | 3.5.1 | Sudah ada di requirements.txt ETS |
| delta-spark | 3.2.0 | Ditambahkan saat upgrade Medallion |

Jar Delta Lake didownload otomatis oleh Ivy saat pertama kali dijalankan (~6MB).
Run berikutnya menggunakan cache di `~/.ivy2/cache/`.

---

## Troubleshooting

**Error: `delta` format not recognized**
→ Pastikan `PYSPARK_SUBMIT_ARGS` sudah di-set (Step 2)

**Error: `Connection refused namenode:9000`**
→ Container namenode belum siap. Jalankan `docker-compose ps` dan tunggu status `healthy`

**Error: `Path does not exist lakehouse_data/bronze/...`**
→ Jalankan script secara berurutan: bronze → silver → gold
