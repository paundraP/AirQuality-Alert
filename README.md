# AirQuality-Alert
### Sistem Pemantauan Kualitas Udara Jawa Timur

Pipeline Big Data untuk memantau Air Quality Index (AQI) kota-kota di Jawa Timur secara real-time, menganalisis pola polusi dengan Apache Spark, dan menampilkan hasil di dashboard web.

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
4. **Menganalisis dengan Apache Spark** — Tiga analisis utama (klasifikasi AQI, jam puncak polusi, ranking kota) plus prediksi MLlib
5. **Menampilkan di dashboard web** — Visualisasi interaktif berbasis Flask dengan desain modern dark-mode

---

## Arsitektur Sistem

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
         │                               │
         └───────────┬───────────────────┘
                     ▼
         ┌───────────────────────┐
         │ consumer_to_hdfs.py   │
         └─────┬───────────┬─────┘
               │           │
               ▼           ▼
     ┌──────────────┐  ┌──────────────────┐
     │ HDFS Storage │  │ dashboard/data/  │
     │ (Hadoop)     │  │ (local copy)     │
     └──────┬───────┘  └────────┬─────────┘
            │                   │
            ▼                   ▼
   ┌────────────────┐  ┌────────────────────┐
   │ analysis.ipynb │  │ dashboard/app.py   │
   └──────┬─────────┘  └────────┬───────────┘
          │                     │
          ▼                     ▼
 ┌──────────────────┐  ┌────────────────────┐
 │ spark_results    │──│ localhost:5000     │
 │ .json            │  │ Dashboard Web UI   │
 └──────────────────┘  └────────────────────┘
```

---

## Teknologi yang Digunakan

| Komponen | Teknologi | Versi |
|---|---|---|
| Message Broker | Apache Kafka (Confluent) | 7.4.0 |
| Koordinasi | Apache ZooKeeper | 7.4.0 |
| Distributed Storage | Hadoop HDFS | 3.2.1 |
| Batch Processing | Apache Spark (PySpark) | 3.5.1 |
| Machine Learning | Spark MLlib | 3.5.1 |
| Dashboard | Flask + Vanilla JS | — |
| Containerization | Docker Compose | 3.8 |
| Bahasa | Python | 3.11 |
| Monitoring Kafka | Kafka UI (Provectus) | latest |

**Library Python:** `kafka-python`, `requests`, `feedparser`, `pyspark`, `flask`, `hdfs`, `pandas`

---

## Struktur Direktori

```
AirQuality-Alert/
├── docker-compose-kafka.yml          # Kafka + ZooKeeper + Kafka UI
├── docker-compose-hadoop.yml         # HDFS (NameNode, DataNode, YARN) + Consumer
├── hadoop.env                        # Konfigurasi environment Hadoop
│
├── kafka/                            # Data Ingestion Layer
│   ├── Dockerfile                    # Image consumer (Python 3.11)
│   ├── producer_api.py               # Producer: API ISPU → Kafka topic
│   ├── producer_rss.py               # Producer: RSS feed berita → Kafka topic
│   ├── consumer_to_hdfs.py           # Consumer: Kafka → HDFS + local copy
│   └── logs/                         # Log consumer runtime
│
├── spark/                            # Data Processing & Analysis Layer
│   ├── analysis.ipynb                # Notebook Spark (semua analisis)
│   ├── laporan.md                    # Laporan detail hasil analisis
│   ├── Hasil.md                      # Ringkasan hasil & kesimpulan
│   ├── spark_results_schema.md       # Kontrak format JSON (A4 ↔ A5)
│   └── spark_results.example.json   # Contoh output untuk testing
│
├── dashboard/                        # Presentation Layer
│   ├── app.py                        # Flask server (port 5000)
│   ├── templates/index.html          # Dashboard UI (dark mode)
│   ├── static/style.css              # Styling (glassmorphism)
│   └── data/                         # Data auto-generated (gitignored)
│       ├── api/                      # Live AQI data
│       ├── rss/                      # Berita lingkungan
│       └── spark_results.json        # Output analisis Spark
│
├── scripts/                          # Automation Scripts
│   ├── init-kafka-topics.sh          # Setup topics Kafka + direktori HDFS
│   ├── run-producers-and-wait-hdfs.sh
│   ├── run-spark-analysis.sh         # Eksekusi notebook Spark di container
│   └── run-all.sh                    # Orchestrator end-to-end pipeline
│
├── sent_articles.json                # Cache anti-duplikat RSS (gitignored)
└── README.md
```

---

## Cara Menjalankan

### Prasyarat

- Docker & Docker Compose terinstall
- Python 3.11+ (untuk menjalankan producer di host)
- Koneksi internet (untuk akses API ISPU & RSS feed)

### Langkah per Langkah

**1. Jalankan infrastruktur Hadoop:**
```bash
docker compose -f docker-compose-hadoop.yml up -d
```

**2. Jalankan infrastruktur Kafka:**
```bash
docker compose -f docker-compose-kafka.yml up -d
```

> Verifikasi: `docker ps` — harus ada container `zookeeper`, `kafka-broker`, `kafka-ui`, `namenode`, `datanode`, `consumer`, dsb.

**3. Inisialisasi Kafka topics dan direktori HDFS:**
```bash
bash scripts/init-kafka-topics.sh
```

**4. Install dependensi Python (di host):**
```bash
pip install kafka-python requests feedparser pyspark flask
```

**5. Jalankan producer (ingestion data):**
```bash
bash scripts/run-producers-and-wait-hdfs.sh
```

**6. Jalankan analisis Spark:**
```bash
bash scripts/run-spark-analysis.sh
```

**7. Jalankan dashboard:**
```bash
cd dashboard && python app.py
```

Buka `http://localhost:5000` di browser.

### One-Command (End-to-End)

```bash
# Pastikan Docker Compose Hadoop & Kafka sudah jalan, lalu:
bash scripts/run-all.sh
```

Flag opsional untuk skip tahapan tertentu:

```bash
SKIP_INIT=1       bash scripts/run-all.sh   # Lewati setup topics + HDFS
SKIP_INGESTION=1  bash scripts/run-all.sh   # Lewati producer
SKIP_SPARK=1      bash scripts/run-all.sh   # Lewati analisis Spark
```

---

## Komponen Sistem

### 1. Kafka Producer — Data Ingestion

**`producer_api.py`** (Topic: `airquality-api`)
- Mengambil data AQI real-time dari API ISPU Kementerian LHK (`ispu.kemenlh.go.id`)
- Memfilter hanya stasiun di Jawa Timur dengan deduplikasi per kota
- Normalisasi nama kota (menghilangkan prefix "Kota"/"Kabupaten")
- Payload: `id_stasiun`, `kota`, `aqi`, `pm25`, `timestamp`, `kategori`

**`producer_rss.py`** (Topic: `airquality-rss`)
- Mengambil berita polusi udara dari 3 sumber RSS: Tempo, Kompas, Detik
- Sistem anti-duplikat berbasis hash MD5 dari URL artikel
- Cache persistent ke `sent_articles.json` agar tidak mengirim ulang antar run
- Payload: `id`, `judul`, `link`, `ringkasan`, `waktu_terbit`, `sumber`

### 2. Kafka Consumer — Penyimpanan HDFS

**`consumer_to_hdfs.py`**
- Subscribe ke 2 topic (`airquality-api`, `airquality-rss`) secara paralel via threading
- Buffer mechanism: mengumpulkan data selama 120 detik sebelum flush ke storage
- Dual write: menyimpan ke HDFS (`/data/airquality/api/` dan `/data/airquality/rss/`) sekaligus ke lokal (`dashboard/data/`)
- Berjalan sebagai Docker container dengan akses ke jaringan Kafka dan Hadoop

### 3. Apache Spark — Analisis Data

**`analysis.ipynb`** — Notebook PySpark yang membaca data dari HDFS dan menjalankan 4 analisis:

| Kode | Analisis | Metode |
|---|---|---|
| AQI-07 | Distribusi kategori AQI per kota | `groupBy` + persentase |
| AQI-08 | Jam puncak polusi per kota | `groupBy(kota, jam)` + `max(avg_aqi)` |
| AQI-09 | Ranking kota AQI terburuk | Window function `rank()` |
| AQI-B2 | Prediksi AQI 24 jam (bonus) | `LinearRegression` MLlib |

Output disimpan ke `dashboard/data/spark_results.json`. Format lengkap: `spark/spark_results_schema.md`.

### 4. Flask Dashboard — Visualisasi

- **Panel 1:** Pemantauan AQI real-time — tabel kota dengan badge warna kategori
- **Panel 2:** Berita lingkungan terbaru dari RSS feed
- **Panel 3:** Hasil analisis Spark — ranking kota terburuk + distribusi kategori (progress bar visual)
- **Desain:** Dark mode, glassmorphism, Google Fonts (Inter), animasi pulse, responsive grid 12 kolom
- **Auto-refresh:** data diperbarui setiap 30 detik via `fetch('/api/data')`

---

## Hasil Analisis

### Analisis 1: Klasifikasi AQI per Kota

Skala AQI: Baik (0–50) · Sedang (51–100) · Tidak Sehat (101–200) · Berbahaya (>200)

| Kota | Kategori | Persentase (%) |
|---|---|:---:|
| Banyuwangi | Baik | 100.0 |
| Lumajang | Baik | 100.0 |
| Jombang | Baik / Sedang | 66.7 / 33.3 |
| Malang | Baik / Sedang | 66.7 / 33.3 |
| Pasuruan | Baik / Sedang | 66.7 / 33.3 |
| Bojonegoro | Sedang | 100.0 |
| Madiun | Sedang | 100.0 |
| Mojokerto | Sedang | 100.0 |
| Probolinggo | Sedang | 100.0 |
| Surabaya | Sedang | 100.0 |

> Tidak ada kota yang masuk kategori Tidak Sehat atau Berbahaya pada periode pengukuran.

### Analisis 2: Jam Puncak Polusi per Kota

| Kota | Jam Puncak | Sesi | AQI Puncak |
|---|:---:|---|:---:|
| Probolinggo | 05.00 | Dini Hari (00–06) | 71.0 |
| Surabaya | 10.00 | Siang (10–16) | 67.0 |
| Madiun | 05.00 | Dini Hari (00–06) | 66.0 |
| Bojonegoro | 10.00 | Siang (10–16) | 57.0 |
| Pasuruan | 10.00 | Siang (10–16) | 57.0 |
| Mojokerto | 05.00 | Dini Hari (00–06) | 56.0 |
| Malang | 10.00 | Siang (10–16) | 54.0 |
| Jombang | 10.00 | Siang (10–16) | 51.0 |
| Banyuwangi | 05.00 | Dini Hari (00–06) | 32.0 |
| Lumajang | 05.00 | Dini Hari (00–06) | 25.0 |

Dua pola dominan yang ditemukan:

| Pola | Kota | Dugaan Penyebab |
|---|---|---|
| Dini Hari (05.00) | Probolinggo, Madiun, Mojokerto, Banyuwangi, Lumajang | Aktivitas industri / pembakaran sebelum subuh |
| Siang (10.00) | Surabaya, Bojonegoro, Pasuruan, Malang, Jombang | Akumulasi polutan pagi + efek panas matahari |

### Analisis 3: Ranking Kota AQI Terburuk

Ranking menggunakan window function `rank()` berdasarkan rata-rata AQI dan jumlah event AQI > 100.

| Peringkat | Kota | Avg AQI | Max AQI | Min AQI | Event Tidak Sehat | Total Data |
|:---:|---|:---:|:---:|:---:|:---:|:---:|
| 1 | Probolinggo | 71.0 | 71 | 71 | 0 | 3 |
| 2 | Madiun | 66.0 | 66 | 66 | 0 | 3 |
| 3 | Surabaya | 63.7 | 67 | 62 | 0 | 3 |
| 4 | Mojokerto | 55.3 | 56 | 54 | 0 | 3 |
| 5 | Bojonegoro | 54.0 | 57 | 52 | 0 | 3 |
| 6 | Pasuruan | 52.3 | 57 | 50 | 0 | 3 |
| 7 | Malang | 46.7 | 54 | 42 | 0 | 3 |
| 8 | Jombang | 45.7 | 51 | 41 | 0 | 3 |
| 9 | Banyuwangi | 29.7 | 32 | 25 | 0 | 3 |
| 10 | Lumajang | 25.0 | 25 | 25 | 0 | 3 |

Temuan utama: Probolinggo, Madiun, dan Surabaya adalah 3 kota teratas dan kandidat prioritas intervensi. Lumajang & Banyuwangi konsisten kategori Baik dan dapat dijadikan benchmark. Semua kota memiliki `event_tidak_sehat = 0`, wajar untuk Jawa Timur di periode ini.

### Bonus: Prediksi AQI dengan Spark MLlib

| Aspek | Detail |
|---|---|
| Algoritma | Linear Regression |
| Fitur | `jam` (0–23) + `kota` (StringIndexer → OneHotEncoder) |
| Target | Nilai AQI |
| Split Data | 80:20 (seed = 42) |
| Output | 5 kota × 24 jam = 120 baris prediksi |

> Akurasi model masih terbatas (R² rendah) karena data historis di HDFS belum cukup banyak. Setelah producer berjalan beberapa hari, model bisa di-retrain dengan fitur tambahan (cuaca, lag AQI) dan algoritma yang lebih kuat (`GBTRegressor`).

---

## Kesimpulan & Rekomendasi

### Kesimpulan

1. Kualitas udara Jawa Timur secara umum masih dalam kategori Baik–Sedang — tidak ada kota yang masuk Tidak Sehat atau Berbahaya pada periode April–Mei 2026.
2. Pola polusi terbagi dua: dini hari (jam 05) untuk kota-kota industri dan siang (jam 10) untuk kota-kota urban besar.
3. Probolinggo (AQI 71) menjadi kota dengan kualitas udara terburuk, diikuti Madiun dan Surabaya.
4. Pipeline big data berhasil diimplementasikan end-to-end: dari ingestion (Kafka) → storage (HDFS) → processing (Spark) → visualization (Dashboard).

### Rekomendasi untuk Dinas Kesehatan

| No | Rekomendasi | Target |
|:---:|---|---|
| 1 | **Notifikasi proaktif** — Kirim peringatan ISPU 30 menit sebelum jam puncak per kota | Seluruh kota |
| 2 | **Jadwal aktivitas luar ruang** — Olahraga setelah jam 08 untuk kota puncak dini hari; hindari siang untuk kota puncak jam 10 | Masyarakat |
| 3 | **Koordinasi industri** — Batasi pembakaran di malam dan dini hari | Probolinggo, Madiun |
| 4 | **Penguatan pemantauan** — Prioritaskan sensor real-time tambahan | Probolinggo, Surabaya |
| 5 | **Edukasi berbasis lokal** — Jadwal penyuluhan disesuaikan per kota | Dinas Kesehatan |

---

## Port Mapping

| Port | Service | URL |
|:---:|---|---|
| 9870 | HDFS NameNode Web UI | http://localhost:9870 |
| 9000 | HDFS RPC | — |
| 8088 | YARN ResourceManager | http://localhost:8088 |
| 9092 | Kafka Broker | — |
| 8080 | Kafka UI | http://localhost:8080 |
| 5000 | Flask Dashboard | http://localhost:5000 |

---

## Catatan Penting

### Validitas Data

Data saat ini masih terbatas — hasil analisis **belum bisa dijadikan kesimpulan final** karena:

- Setiap slot jam hanya memiliki 1 data point (`jumlah_data = 1`) — idealnya puluhan hingga ratusan
- Data hanya mencakup 3 waktu pengukuran (jam 05, 07, 10) — belum 24 jam penuh
- Producer belum berjalan kontinu sepanjang hari

Untuk hasil yang valid secara statistik, producer perlu dijalankan secara terjadwal (setiap 1 jam) selama minimal 7–14 hari agar setiap slot jam memiliki cukup data.

### Output untuk Dashboard

File `dashboard/data/spark_results.json` berisi semua hasil analisis:

| Key | Sumber | Deskripsi |
|---|---|---|
| `distribusi_kategori` | Analisis 1 | Persentase kategori AQI per kota |
| `aqi_per_jam` | Analisis 2 | Rata-rata AQI per kota per jam |
| `jam_puncak_per_kota` | Analisis 2 | Jam dengan AQI tertinggi per kota |
| `ranking_kota` | Analisis 3 | Peringkat kota berdasarkan rata-rata AQI |
| `prediksi_aqi` | Bonus MLlib | Prediksi AQI 24 jam (5 kota × 24 jam) |
| `model_metrics` | Bonus MLlib | RMSE, R², jumlah data train/test |
| `generated_at` | — | Timestamp pembuatan file |

Format lengkap: [`spark/spark_results_schema.md`](spark/spark_results_schema.md)

---

*AirQuality-Alert — Big Data Pipeline untuk Kualitas Udara yang Lebih Baik*