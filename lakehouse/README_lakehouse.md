# README_lakehouse.md
# Upgrade Pipeline ETS → Data Lakehouse (Medallion Architecture)

**Topik:** AirQuality Alert — Pemantauan Kualitas Udara Jawa Timur  
**Mata Kuliah:** Big Data dan Data Lakehouse

---

## Diagram Arsitektur: Sebelum vs Sesudah

### Sebelum (ETS)

```
[ISPU API] ──► producer_api.py ──► Kafka (airquality-api) ──┐
                                                              ├──► consumer_to_hdfs.py ──► HDFS (JSON mentah)
[RSS Feed] ──► producer_rss.py ──► Kafka (airquality-rss) ──┘
                                                                        │
                                                                        ▼
                                                               analysis.py (PySpark)
                                                               - Baca JSON dari HDFS
                                                               - Analisis langsung
                                                               - Tulis hasil ke HDFS
                                                                        │
                                                                        ▼
                                                               spark_results.json
                                                                        │
                                                                        ▼
                                                               Flask Dashboard
```

**Masalah di arsitektur ETS:**
- Tidak ada ACID — jika proses Spark crash saat tulis, file rusak sebagian
- Tidak ada schema enforcement — kolom bisa berubah sewaktu-waktu tanpa notifikasi
- Tidak ada versioning — data lama hilang setiap kali overwrite
- Data mentah dan hasil analisis tercampur di HDFS

---

### Sesudah (Medallion Lakehouse)

```
[ISPU API] ──► Kafka ──► HDFS (JSON mentah)
                                 │
                    ─────────────┘  lakehouse/01_bronze.py
                    │
                    ▼
          ┌─────────────────────┐
          │  BRONZE Delta Lake  │  ← raw data + _ingested_at + _source
          │  (append only)      │    ACID guaranteed, schema tracked
          └─────────┬───────────┘
                    │  lakehouse/02_silver.py
                    ▼
          ┌─────────────────────┐
          │  SILVER Delta Lake  │  ← cleaned, typed, deduplicated
          │  (overwrite/rebuild)│    7 transformasi terdokumentasi
          └─────────┬───────────┘
                    │  lakehouse/03_gold.py
                    ▼
          ┌─────────────────────────────────────────────────┐
          │  GOLD Delta Lake                                │
          │  G1: aqi_category_dist  (Reproduksi ETS A1)    │
          │  G2: aqi_hourly         (Reproduksi ETS A2)    │
          │  G3: aqi_ranking        (Reproduksi ETS A3)    │
          │  G4: aqi_trend          ★ ENHANCED (Window lag)│
          │  G5: aqi_alert_hours    ★ ENHANCED (alert)     │
          └─────────┬───────────────────────────────────────┘
                    │
                    ▼
          Flask Dashboard (baca dari Gold via spark_results.json)
```

---

## Penjelasan Transformasi Silver

Script `02_silver.py` menerapkan 7 transformasi. Berikut justifikasi setiap transformasi:

### T1 — Cast tipe data
**Apa:** `aqi` dan `pm25` dicast ke `DoubleType`; `ingested_at` ke `TimestampType`  
**Kenapa:** JSON dari HDFS tidak punya tipe — semua angka masuk sebagai `LongType` atau `StringType`. Operasi `avg()`, `max()`, Window Function `.orderBy()` membutuhkan tipe yang tepat. Jika tidak dicast, Spark akan error atau menghasilkan angka yang salah.

### T2 — Ekstrak jam dari timestamp
**Apa:** Buat kolom `jam` berisi angka 0–23 dari kolom `ts`  
**Kenapa:** Analisis jam puncak polusi membutuhkan kolom jam. Dengan menyimpannya di Silver, semua analisis downstream tidak perlu hitung ulang — satu definisi konsisten untuk seluruh pipeline.

### T3 — Standarisasi nama kota
**Apa:** `upper(trim(col("kota")))` — hapus spasi dan jadikan huruf kapital  
**Kenapa:** API ISPU kadang mengirim `"Surabaya"`, `"SURABAYA"`, `"surabaya "`. Tanpa standarisasi, `groupBy("kota")` menghasilkan 3 baris berbeda untuk kota yang sama. Ini menyebabkan ranking ganda dan rata-rata yang salah.

### T4 — Filter baris null pada kolom kritis
**Apa:** Hapus baris di mana `kota` atau `aqi` null  
**Kenapa:** Baris tanpa kota tidak bisa diatribusikan ke kota mana pun — tidak berguna untuk analisis geografis. Baris tanpa AQI adalah data yang belum lengkap dari sensor. Menyertakannya akan mendistorsi agregasi.

### T5 — Filter nilai AQI anomali
**Apa:** Hapus baris di mana `aqi < 0` atau `aqi > 999`  
**Kenapa:** AQI negatif tidak mungkin secara fisik. AQI > 999 melebihi skala ISPU Indonesia (maksimum 300 untuk kategori Berbahaya). Nilai ini kemungkinan error sensor, parsing error, atau data uji. Menyertakannya mendistorsi rata-rata dan bisa membuat ranking kota salah.

### T6 — Deduplikasi
**Apa:** `dropDuplicates(["kota", "ingested_at"])`  
**Kenapa:** Kafka consumer bisa mengirim data yang sama dua kali jika ada network timeout dan retry. Karena Bronze menggunakan mode `append`, duplikat menumpuk setiap run. Silver adalah titik pertama kita enforce "satu record per kota per waktu ingest".

### T7 — Kategorisasi AQI
**Apa:** Tambah kolom `kategori_aqi` berdasarkan skala ISPU Indonesia  
**Kenapa:** Tanpa ini, setiap script analisis mendefinisikan threshold sendiri — risiko inkonsistensi. Dengan menyimpan di Silver, seluruh pipeline (Gold, Dashboard) menggunakan definisi yang sama. Jika threshold berubah (misalnya kebijakan KLHK update), cukup ubah satu tempat.

---

## Perbandingan Gold vs Analisis Spark ETS

| Aspek | ETS (analysis.py) | Lakehouse (03_gold.py) |
|-------|-------------------|------------------------|
| Sumber data | JSON mentah HDFS | Silver Delta (sudah bersih) |
| Tipe kolom aqi | String/Long | Double (akurat) |
| Deduplikasi | Tidak ada | Sudah di Silver |
| Kolom jam | Dihitung ulang setiap run | Sudah tersedia di Silver |
| Analisis tren (lag) | Tidak bisa — timestamp String | Bisa — TimestampType di Silver |
| Versioning hasil | Tidak ada | Setiap write = versi baru |
| Recovery jika crash | Data bisa rusak sebagian | ACID — write atomik |
| Reproducibility | Tidak bisa replay | Bisa query versi lama |

### Tabel Enhanced yang tidak ada di ETS

**G4 — aqi_trend (Window Function lag)**

Di ETS, analisis paling jauh yang bisa dilakukan adalah rata-rata AQI per jam. Tidak bisa menjawab pertanyaan "apakah kualitas udara Surabaya membaik atau memburuk dari waktu ke waktu?" karena:
1. Kolom timestamp masih bertipe String → Window `.orderBy("ts")` tidak menghasilkan urutan kronologis yang benar
2. Tidak ada deduplikasi → lag() bisa mengambil duplikat sebagai "record sebelumnya"

Setelah Silver: timestamp sudah `TimestampType`, duplikat sudah dihapus → `lag("aqi", 1).over(Window.partitionBy("kota").orderBy("ts"))` menghasilkan tren yang akurat.

**G5 — aqi_alert_hours**

Menghitung persentase waktu setiap kota dalam kondisi "Tidak Sehat" (AQI > 100). Ini tidak ada di ETS karena analisis ETS hanya melihat rata-rata, bukan durasi kondisi berbahaya. Informasi ini lebih relevan untuk rekomendasi kesehatan: kota yang rata-rata AQI-nya 90 tapi 40% waktunya di atas 100 lebih berbahaya dari kota yang rata-rata 95 tapi selalu stabil.

---

## Refleksi: Keuntungan Delta Lake vs HDFS/CSV

### 1. ACID Transactions
Di HDFS, jika proses Spark crash di tengah penulisan, file bisa rusak sebagian — ada baris yang terpotong, schema tidak konsisten. Di Delta Lake, setiap write adalah transaksi atomik: berhasil penuh atau tidak sama sekali. Tidak ada "partially written file".

### 2. Time Travel (Schema + Data Versioning)
Ini keuntungan paling konkret yang terasa selama pengerjaan tugas ini. Setiap kali kita menjalankan pipeline, Delta otomatis menyimpan versi sebelumnya di folder `_delta_log/`. Jika kita salah update data, kita bisa query `versionAsOf=0` untuk melihat data asli — hal yang mustahil dilakukan di HDFS biasa.

### 3. Schema Enforcement
Delta Lake menolak write jika schema tidak sesuai (kecuali `mergeSchema=true` diset eksplisit). Di HDFS, jika API ISPU tiba-tiba menambahkan kolom baru atau mengubah nama kolom, Spark bisa crash saat runtime tanpa warning. Delta mendeteksi ini lebih awal.

### 4. Efisiensi Query dengan Partitioning
Delta mendukung Z-Ordering dan data skipping — query `WHERE kota = 'SURABAYA'` tidak perlu scan seluruh file, cukup file yang mengandung data Surabaya. Di HDFS JSON biasa, setiap query harus baca semua file.

### 5. Medallion Architecture = Separation of Concerns
Pisahnya Bronze/Silver/Gold bukan hanya estetika. Bronze menjamin data asli selalu tersedia untuk replay. Silver menjamin kualitas data. Gold menjamin performa query. Jika ada bug di Silver, kita bisa fix logikanya dan rebuild dari Bronze tanpa re-ingest dari sumber asli.

---

## Screenshot Output

*(Jalankan pipeline dan paste screenshot output terminal di sini untuk laporan)*

Output yang perlu di-screenshot:
1. `01_bronze.py` — baris `[BRONZE-API] Selesai. X baris`
2. `02_silver.py` — tabel distribusi kategori AQI
3. `03_gold.py` — output tabel G4 tren dan Time Travel Step 6/7
4. Isi folder `ls -la lakehouse_data/silver/airquality/_delta_log/`

---

## Struktur Folder Output

```
lakehouse_data/
├── bronze/
│   ├── airquality_api/
│   │   ├── part-00000-*.parquet
│   │   └── _delta_log/           ← transaction log (time travel)
│   └── airquality_rss/
│       ├── part-00000-*.parquet
│       └── _delta_log/
├── silver/
│   └── airquality/
│       ├── part-00000-*.parquet
│       └── _delta_log/
└── gold/
    ├── aqi_category_dist/
    ├── aqi_hourly/
    ├── aqi_ranking/
    ├── aqi_trend/            ← Enhanced: Window lag
    └── aqi_alert_hours/      ← Enhanced: alert durasi
```
