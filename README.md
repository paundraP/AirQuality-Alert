# AirQuality-Alert
### Sistem Pemantauan Kualitas Udara Jawa Timur

Pipeline Big Data end-to-end untuk memantau Air Quality Index (AQI) kota-kota di Jawa Timur secara real-time, menganalisis pola polusi dengan Apache Spark + Delta Lake (Medallion Architecture), dan menampilkan hasil di dashboard web.

**Sumber Data:** ISPU Kementerian LHK & RSS Berita Lingkungan  
**Periode Data:** April–Mei 2026  
**Wilayah Cakupan:** 10 kota di Provinsi Jawa Timur

---

## Daftar Isi

- [Gambaran Umum](#gambaran-umum)
- [Arsitektur Sistem](#arsitektur-sistem)
- [Teknologi yang Digunakan](#teknologi-yang-digunakan)
- [Struktur Direktori](#struktur-direktori)
- [Cara Menjalankan](#cara-menjalankan)
- [Komponen Sistem](#komponen-sistem)
- [Lakehouse — Medallion Architecture](#lakehouse--medallion-architecture)
- [Hasil Analisis](#hasil-analisis)
- [Kesimpulan & Rekomendasi](#kesimpulan--rekomendasi)
- [Port Mapping](#port-mapping)
- [Catatan Penting](#catatan-penting)

---

## Gambaran Umum

**AirQuality-Alert** adalah sistem pemantauan kualitas udara berbasis big data yang dirancang untuk:

1. **Mengumpulkan data real-time** — AQI dari API ISPU Kementerian LHK dan berita lingkungan dari RSS feed (Tempo, Kompas, Detik)
2. **Streaming via Apache Kafka** — Data dikirim melalui message broker untuk pemrosesan yang scalable dan fault-tolerant
3. **Menyimpan ke HDFS** — Data persisten di Hadoop Distributed File System untuk analisis batch
4. **Data Lakehouse (Medallion Architecture)** — Data dari HDFS diproses melalui tiga layer Delta Lake: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
5. **Menganalisis dengan Apache Spark** — Tiga analisis utama (klasifikasi AQI, jam puncak polusi, ranking kota) plus prediksi MLlib dan Enhanced Analysis (tren AQI, alert durasi)
6. **Menampilkan di dashboard web** — Visualisasi interaktif berbasis Flask dengan desain modern dark-mode

---

## Arsitektur Sistem

### Pipeline Lengkap (Tugas ETS + Lakehouse)

```
┌─────────────────────┐      ┌──────────────────────────┐
│  ISPU API (LHK)     │      │  RSS Feed Lingkungan     │
│  ispu.kemenlh.go.id │      │  Tempo / Kompas / Detik  │
└────────┬────────────┘      └────────────┬─────────────┘
         │                                │
         ▼                                ▼
┌────────────────────┐      ┌─────────────────────────┐
│ producer_api.py    │      │ producer_rss.py         │
└────────┬───────────┘      └────────────┬────────────┘
         │                               │
         ▼                               ▼
┌────────────────────┐      ┌─────────────────────────┐
│ Topic:             │      │ Topic:                  │
│ airquality-api     │      │ airquality-rss          │
└────────┬───────────┘      └────────────┬────────────┘
         └──────────────┬────────────────┘
                        ▼
            ┌───────────────────────┐
            │  consumer_to_hdfs.py  │
            └─────┬─────────┬───────┘
                  │         │
                  ▼         ▼
        ┌──────────────┐  ┌──────────────────┐
        │ HDFS Storage │  │ dashboard/data/  │
        │ (Hadoop)     │  │ (local copy)     │
        └──────┬───────┘  └────────┬─────────┘
               │                   │
               ▼                   ▼
   ┌───────────────────────┐  ┌────────────────────┐
   │  Medallion Pipeline   │  │  dashboard/app.py  │
   │  (analysis.py)        │  └────────┬───────────┘
   │                       │           │
   │  HDFS JSON            │           ▼
   │     ↓                 │  ┌────────────────────┐
   │  [BRONZE] Delta Lake  │  │  localhost:5001    │
   │     ↓                 │  │  Dashboard Web UI  │
   │  [SILVER] Delta Lake  │  └────────────────────┘
   │     ↓                 │
   │  [GOLD]  Delta Lake   │
   │     ↓                 │
   │  spark_results.json   |
   └───────────────────────┘
```

---

## Teknologi yang Digunakan

| Komponen | Teknologi | Versi |
|---|---|---|
| Message Broker | Apache Kafka (Confluent) | 7.4.0 |
| Koordinasi | Apache ZooKeeper | 7.4.0 |
| Distributed Storage | Hadoop HDFS | 3.2.1 |
| Batch Processing | Apache Spark (PySpark) | 3.5.1 |
| **Data Lakehouse** | **Delta Lake** | **3.2.0** |
| Machine Learning | Spark MLlib | 3.5.1 |
| Dashboard | Flask + Vanilla JS | — |
| Containerization | Docker Compose | — |
| Bahasa | Python | 3.11 |
| Monitoring Kafka | Kafka UI (Provectus) | latest |

**Library Python:** `kafka-python`, `requests`, `feedparser`, `pyspark`, `delta-spark`, `flask`, `hdfs`, `pandas`

---

## Struktur Direktori

```
AirQuality-Alert/
├── docker-compose.yml                # Stack: Kafka + HDFS + Spark + Dashboard
├── Dockerfile                        # Runtime Python
├── requirements.txt                  # Dependensi Python (termasuk delta-spark)
├── hadoop.env                        # Konfigurasi Hadoop
│
├── kafka/                            # Data Ingestion Layer
│   ├── producer_api.py               # Producer: API ISPU → Kafka topic
│   ├── producer_rss.py               # Producer: RSS feed → Kafka topic
│   ├── consumer_to_hdfs.py           # Consumer: Kafka → HDFS + local copy
│   └── logs/
│
├── spark/                            # Data Processing Layer (ETS)
│   ├── analysis.py                   # Pipeline Medallion + analisis + MLlib
│   └── spark_results_schema.md
│
├── lakehouse/                        # Data Lakehouse Layer (BARU)
│   ├── 00_setup.md                   # Panduan menjalankan lakehouse
│   ├── 01_bronze.py                  # HDFS JSON → Bronze Delta Lake
│   ├── 02_silver.py                  # Bronze → Silver (7 transformasi)
│   ├── 03_gold.py                    # Silver → Gold (5 tabel) + Time Travel
│   └── README_lakehouse.md           # Dokumentasi lengkap lakehouse
│
├── delta_lake/                       # Storage Delta Lake (auto-generated)
│   ├── bronze/
│   │   ├── airquality_api/           # Raw API data + metadata
│   │   └── airquality_rss/           # Raw RSS data + metadata
│   ├── silver/
│   │   └── airquality/               # Cleaned, typed, deduplicated
│   └── gold/
│       ├── aqi_category_dist/        # Distribusi kategori per kota
│       ├── aqi_hourly/               # Rata-rata AQI per jam
│       ├── aqi_ranking/              # Ranking kota terburuk
│       ├── aqi_trend/                # Tren AQI (Enhanced)
│       └── aqi_alert_hours/          # Durasi kondisi tidak sehat (Enhanced)
│
├── dashboard/                        # Presentation Layer
│   ├── app.py                        # Flask server (port 5001)
│   ├── templates/index.html          # Dashboard UI (dark mode)
│   ├── static/style.css
│   └── data/
│       ├── api/
│       ├── rss/
│       └── spark_results.json        # Output dari Gold layer
│
├── scripts/
│   └── scheduler.py                  # Orkestrasi pipeline setiap 15 menit
│
└── README.md
```

---

## Cara Menjalankan

### Prasyarat

- Docker & Docker Compose terinstall
- Koneksi internet (untuk akses API ISPU, RSS feed, dan download jar Delta Lake)

### 1. Jalankan Stack Utama

```bash
docker-compose up -d --build
```

Stack ini menjalankan semua komponen secara bersamaan:

| Service | Fungsi |
|---|---|
| `zookeeper`, `kafka-broker`, `kafka-ui` | Infrastruktur Kafka |
| `namenode`, `datanode` | Infrastruktur HDFS |
| `consumer` | Consumer Kafka → HDFS + local copy |
| `scheduler` | Pipeline otomatis setiap 15 menit (ingestion + Medallion Spark) |
| `dashboard` | Flask dashboard di `http://localhost:5001` |

Pantau log scheduler:
```bash
docker-compose logs -f scheduler
```

### 2. Jalankan Pipeline Lakehouse Secara Manual

Untuk menjalankan script lakehouse standalone (demo atau pengujian):

```bash
# Masuk ke container
docker exec -it scheduler bash

# Set env var Delta Lake
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell"

# Jalankan berurutan
cd /app
python3 lakehouse/01_bronze.py   # HDFS → Bronze Delta
python3 lakehouse/02_silver.py   # Bronze → Silver (cleaning)
python3 lakehouse/03_gold.py     # Silver → Gold + Time Travel demo
```

### Alur Pipeline Otomatis (Setiap 15 Menit)

```
scheduler.py
  │
  ├─ 1. producer_api.py    → kirim AQI ke Kafka
  ├─ 2. producer_rss.py    → kirim berita ke Kafka
  ├─ 3. tunggu 75s         → consumer flush ke HDFS
  └─ 4. analysis.py        → Medallion pipeline + export spark_results.json
                              (Bronze → Silver → Gold → Dashboard)
```

### Port Utama

| URL | Keterangan |
|---|---|
| `http://localhost:5001` | Dashboard Flask |
| `http://localhost:8080` | Kafka UI |
| `http://localhost:9870` | Hadoop NameNode UI |

---

## Komponen Sistem

### 1. Kafka Producer — Data Ingestion

**`producer_api.py`** (Topic: `airquality-api`)
- Mengambil data AQI real-time dari API ISPU Kementerian LHK
- Memfilter 10 stasiun di Jawa Timur dengan deduplikasi per kota
- Normalisasi nama kota (menghilangkan prefix "Kota"/"Kabupaten")
- Payload: `id_stasiun`, `kota`, `aqi`, `pm25`, `timestamp`, `kategori`

**`producer_rss.py`** (Topic: `airquality-rss`)
- Mengambil berita polusi dari 3 RSS: Tempo, Kompas, Detik
- Anti-duplikat berbasis hash MD5
- Cache persistent ke `dashboard/data/sent_articles.json`

### 2. Kafka Consumer — Penyimpanan HDFS

**`consumer_to_hdfs.py`**
- Subscribe ke 2 topic secara paralel via threading
- Buffer 60 detik sebelum flush
- Dual write: HDFS + `dashboard/data/`

### 3. Medallion Pipeline — `spark/analysis.py`

Script PySpark yang berjalan otomatis setiap 15 menit via scheduler:

| Tahap | Proses | Output |
|---|---|---|
| Bronze | HDFS JSON → Delta Lake raw | `/app/delta_lake/bronze/` |
| Silver | Cleaning 7 transformasi | `/app/delta_lake/silver/` |
| Gold | 3 tabel agregasi | `/app/delta_lake/gold/` |
| Export | Gold → JSON | `dashboard/data/spark_results.json` |

### 4. Lakehouse Scripts — `lakehouse/`

Script standalone untuk demo dan pengujian terpisah dari pipeline otomatis:

| Script | Fungsi |
|---|---|
| `01_bronze.py` | Ingest HDFS → Bronze Delta (API + RSS) |
| `02_silver.py` | 7 transformasi cleaning → Silver Delta |
| `03_gold.py` | 5 tabel Gold + demonstrasi Time Travel |

### 5. Flask Dashboard

- **Peta Leaflet** — marker AQI per kota Jawa Timur dengan warna kategori
- **Tabel real-time** — 10 stasiun dengan badge kategori dan waktu relatif
- **Berita RSS** — artikel lingkungan terbaru (deduplicated)
- **Chart Spark** — ranking kota (bar chart) + distribusi kategori (progress bar)
- **Auto-refresh** setiap 30 detik

---

## Lakehouse — Medallion Architecture

### Perbandingan ETS vs Lakehouse

| Aspek | ETS (HDFS + Spark biasa) | Lakehouse (Delta Lake) |
|---|---|---|
| Format storage | JSON mentah | Delta (Parquet + transaction log) |
| ACID | ❌ Tidak ada | ✅ Penuh |
| Schema enforcement | ❌ Tidak ada | ✅ Ada, error jika mismatch |
| Versioning | ❌ Overwrite = hilang | ✅ Setiap write = versi baru |
| Time Travel | ❌ Tidak bisa | ✅ Query versi mana pun |
| Deduplikasi | ❌ Manual tiap analisis | ✅ Dilakukan sekali di Silver |
| Tipe data | ❌ Semua String/Long | ✅ Benar sejak Silver |

### Bronze Layer

Menyimpan data mentah dari HDFS ke Delta Lake **tanpa transformasi logis**, hanya tambah metadata:
- `_ingested_at` — waktu data masuk lakehouse
- `_source` — `"api"` atau `"rss"`

Mode `append` — data lama tidak pernah ditimpa, selalu bisa di-replay.

### Silver Layer — 7 Transformasi

| # | Transformasi | Alasan |
|---|---|---|
| T1 | Cast `aqi`, `pm25` → Double; `ingested_at` → Timestamp | Analisis numerik dan Window Function butuh tipe benar |
| T2 | Ekstrak `jam` dari timestamp | Tersedia sekali untuk semua analisis downstream |
| T3 | `upper(trim(kota))` | Cegah duplikat kota karena perbedaan kapitalisasi |
| T4 | Filter null pada `kota` dan `aqi` | Baris tidak lengkap tidak bisa dianalisis |
| T5 | Filter AQI tidak dalam range 0–999 | Nilai anomali mendistorsi rata-rata dan ranking |
| T6 | `dropDuplicates(["kota", "ingested_at"])` | Hapus retry duplikat dari Kafka consumer |
| T7 | Tambah kolom `kategori_aqi` | Definisi threshold satu tempat, konsisten di semua analisis |

### Gold Layer — 5 Tabel

| Tabel | Tipe | Isi |
|---|---|---|
| `aqi_category_dist` | Reproduksi ETS | Distribusi % kategori AQI per kota |
| `aqi_hourly` | Reproduksi ETS | Rata-rata AQI per kota per jam |
| `aqi_ranking` | Reproduksi ETS | Ranking kota berdasarkan avg AQI |
| `aqi_trend` | ⭐ Enhanced | Tren membaik/memburuk/stabil per kota (Window `lag()`) |
| `aqi_alert_hours` | ⭐ Enhanced | % waktu kota dalam kondisi Tidak Sehat |

### Time Travel

Delta Lake menyimpan setiap versi tabel secara otomatis di folder `_delta_log/`. Hasil demonstrasi:

```
Versi terbaru : 100 baris (data dari 6 run pipeline)
Versi 0       :  60 baris (run pertama, 30 Mei 2026 12:54)
```

Query versi lama: `spark.read.format("delta").option("versionAsOf", 0).load(path)`

---

## Hasil Analisis

### Analisis 1: Distribusi Kategori AQI per Kota (Gold G1)

Skala AQI ISPU: Baik (0–50) · Sedang (51–100) · Tidak Sehat (101–200) · Berbahaya (>200)

| Kota | Baik (%) | Sedang (%) | Tidak Sehat (%) |
|---|:---:|:---:|:---:|
| Banyuwangi | 100.0 | — | — |
| Lumajang | 100.0 | — | — |
| Jombang | 90.0 | 10.0 | — |
| Malang | 90.0 | 10.0 | — |
| Pasuruan | 90.0 | 10.0 | — |
| Bojonegoro | — | 100.0 | — |
| Madiun | — | 100.0 | — |
| Mojokerto | — | 100.0 | — |
| Probolinggo | — | 100.0 | — |
| Surabaya | — | 80.0 | **20.0** |

> **Temuan baru vs ETS:** Surabaya kini masuk kategori Tidak Sehat 20% waktu pengukuran — hal yang tidak terdeteksi di ETS karena data masih sedikit.

### Analisis 2: Rata-rata AQI per Jam (Gold G2)

Jam puncak polusi per kota berdasarkan 100 observasi:

| Kota | Jam Puncak | AQI Puncak |
|---|:---:|:---:|
| Surabaya | 12 | 91.0 |
| Bojonegoro | 10 | 57.0 |
| Probolinggo | 1, 5 | 71.0 |
| Madiun | 5 | 66.0 |
| Mojokerto | 12 | 84.0 |

### Analisis 3: Ranking Kota AQI Terburuk (Gold G3)

| Peringkat | Kota | Avg AQI | Max AQI | Event Tidak Sehat | Total Data |
|:---:|---|:---:|:---:|:---:|:---:|
| 1 | **Surabaya** | 86.2 | 106 | **2** | 10 |
| 2 | Probolinggo | 71.0 | 71 | 0 | 10 |
| 3 | Mojokerto | 70.6 | 84 | 0 | 10 |
| 4 | Madiun | 66.0 | 66 | 0 | 10 |
| 5 | Bojonegoro | 52.4 | 57 | 0 | 10 |
| 6 | Pasuruan | 46.5 | 57 | 0 | 10 |
| 7 | Jombang | 43.4 | 51 | 0 | 10 |
| 8 | Malang | 40.7 | 54 | 0 | 10 |
| 9 | Lumajang | 37.2 | 43 | 0 | 10 |
| 10 | Banyuwangi | 31.3 | 35 | 0 | 10 |

> **Perubahan dari ETS:** Surabaya naik ke peringkat 1 (sebelumnya peringkat 3) karena data lebih banyak menangkap jam puncak siang hari dengan AQI hingga 106.

### Enhanced Analysis: Tren AQI per Kota (Gold G4)

Analisis yang tidak bisa dilakukan di ETS karena butuh TimestampType yang benar:

| Kota | Membaik | Memburuk | Stabil |
|---|:---:|:---:|:---:|
| Surabaya | — | 2 | 7 |
| Mojokerto | — | 2 | 7 |
| Pasuruan | 2 | — | 7 |
| Malang | 2 | 1 | 6 |
| Bojonegoro | — | — | 9 |

> Surabaya dan Mojokerto cenderung memburuk — perlu perhatian lebih.

### Enhanced Analysis: Durasi Kondisi Tidak Sehat (Gold G5)

| Kota | Total Jam Tidak Sehat | % Waktu | Status |
|---|:---:|:---:|---|
| Surabaya | 2 | 20.0% | Normal |
| Lainnya | 0 | 0.0% | Normal |

### Bonus: Prediksi AQI dengan Spark MLlib

| Aspek | Detail |
|---|---|
| Algoritma | Linear Regression |
| Fitur | `jam` (0–23) + `kota` (one-hot encoding) |
| Split Data | 80:20 (seed=42) |
| Status | Berjalan, akurasi meningkat seiring bertambahnya data |

---

## Kesimpulan & Rekomendasi

### Kesimpulan

1. **Surabaya menjadi kota dengan kualitas udara terburuk** (avg AQI 86.2, max 106) — berbeda dari laporan ETS yang menempatkan Probolinggo di peringkat 1. Perbedaan ini karena data lakehouse lebih banyak dan tidak mengandung duplikat.
2. Surabaya satu-satunya kota yang pernah masuk kategori **Tidak Sehat** (20% waktu pengukuran) — temuan baru yang tidak ada di ETS.
3. Pola polusi tetap terbagi dua: **dini hari** (Probolinggo, Madiun) dan **siang hari** (Surabaya, Mojokerto).
4. Banyuwangi dan Lumajang konsisten sebagai kota paling bersih — benchmark kualitas udara Jawa Timur.
5. Pipeline berhasil diupgrade dari HDFS JSON biasa ke **Data Lakehouse** dengan ACID, versioning, dan Time Travel — semua berjalan dalam satu stack Docker.

### Rekomendasi untuk Dinas Kesehatan

| No | Rekomendasi | Target |
|:---:|---|---|
| 1 | **Prioritas intervensi** — Surabaya butuh monitoring sensor lebih banyak, bukan hanya Probolinggo | Surabaya |
| 2 | **Notifikasi proaktif** — Kirim peringatan 30 menit sebelum jam puncak per kota | Seluruh kota |
| 3 | **Jadwal aktivitas luar ruang** — Hindari jam 10–13 di Surabaya dan Mojokerto | Masyarakat |
| 4 | **Koordinasi industri** — Batasi pembakaran dini hari di Probolinggo dan Madiun | Industri |
| 5 | **Edukasi berbasis tren** — Gunakan data G4 (tren) untuk komunikasi risiko yang lebih akurat | Dinas Kesehatan |

---

## Port Mapping

| Port | Service | URL |
|:---:|---|---|
| 9870 | HDFS NameNode Web UI | http://localhost:9870 |
| 9000 | HDFS RPC | — |
| 9092 | Kafka Broker | — |
| 29092 | Kafka Broker (internal) | — |
| 8080 | Kafka UI | http://localhost:8080 |
| 5001 | Flask Dashboard | http://localhost:5001 |

---

## Catatan Penting

### Validitas Data

Data saat ini 100 observasi per kota (10 kota × 10 run). Untuk hasil yang valid secara statistik, idealnya:
- Minimal 7–14 hari data kontinu
- Setiap slot jam (0–23) memiliki minimal 30 observasi

### Delta Lake Storage

Folder `delta_lake/` di-mount dari host ke container via Docker volume. Data persisten meskipun container di-restart. Untuk mereset:
```bash
docker-compose down
rm -rf delta_lake/bronze delta_lake/silver delta_lake/gold
docker-compose up -d
```

### Jar Delta Lake

Jar `delta-spark_2.12-3.2.0.jar` didownload otomatis oleh Ivy saat pertama kali dijalankan dan di-cache di `~/.ivy2/cache/` di dalam container. Run berikutnya menggunakan cache — tidak perlu koneksi internet lagi.

---

*AirQuality-Alert — Big Data Pipeline + Data Lakehouse untuk Kualitas Udara yang Lebih Baik*