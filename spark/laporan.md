# Laporan Analisis Kualitas Udara Jawa Timur
**Sumber data**: ISPU Kementerian LHK | **Periode**: April–Mei 2026

---

## Analisis 1: Klasifikasi AQI per Kota

### Data AQI Real-time (Snapshot Terbaru)

| Kota | AQI | Waktu Ingested | Jam |
|------|-----|----------------|-----|
| Probolinggo | 71.0 | 2026-05-01 05:44:21 | 5 |
| Madiun | 66.0 | 2026-05-01 05:44:23 | 5 |
| Surabaya | 62.0 | 2026-05-01 05:44:30 | 5 |
| Mojokerto | 56.0 | 2026-05-01 05:44:32 | 5 |
| Bojonegoro | 53.0 | 2026-05-01 05:44:34 | 5 |

### Distribusi Kategori AQI per Kota (Seluruh Data)

| Kota | Kategori | Persentase (%) |
|------|----------|----------------|
| Banyuwangi | Baik | 100.0 |
| Bojonegoro | Sedang | 100.0 |
| Jombang | Baik | 66.7 |
| Jombang | Sedang | 33.3 |
| Lumajang | Baik | 100.0 |
| Madiun | Sedang | 100.0 |
| Malang | Baik | 66.7 |
| Malang | Sedang | 33.3 |
| Mojokerto | Sedang | 100.0 |
| Pasuruan | Baik | 66.7 |
| Pasuruan | Sedang | 33.3 |
| Probolinggo | Sedang | 100.0 |
| Surabaya | Sedang | 100.0 |

> **Catatan**: Skala AQI — Baik (0–50) · Sedang (51–100) · Tidak Sehat (101–200) · Berbahaya (>200)

---

## Analisis 2: Identifikasi Jam Puncak Polusi per Kota

### Rata-rata AQI per Kota per Jam

| Kota | Jam | Avg AQI | Jumlah Data |
|------|-----|---------|-------------|
| Banyuwangi | 05 | 32.0 | 1 |
| Banyuwangi | 07 | 32.0 | 1 |
| Banyuwangi | 10 | 25.0 | 1 |
| Bojonegoro | 10 | 57.0 | 1 |
| Bojonegoro | 05 | 53.0 | 1 |
| Bojonegoro | 07 | 52.0 | 1 |
| Jombang | 10 | 51.0 | 1 |
| Jombang | 05 | 45.0 | 1 |
| Jombang | 07 | 41.0 | 1 |
| Lumajang | 05 | 25.0 | 1 |
| Lumajang | 07 | 25.0 | 1 |
| Lumajang | 10 | 25.0 | 1 |
| Madiun | 05 | 66.0 | 1 |
| Madiun | 10 | 66.0 | 1 |
| Madiun | 07 | 66.0 | 1 |
| Malang | 10 | 54.0 | 1 |
| Malang | 07 | 44.0 | 1 |
| Malang | 05 | 42.0 | 1 |
| Mojokerto | 05 | 56.0 | 1 |
| Mojokerto | 07 | 56.0 | 1 |
| Mojokerto | 10 | 54.0 | 1 |
| Pasuruan | 10 | 57.0 | 1 |
| Pasuruan | 05 | 50.0 | 1 |
| Pasuruan | 07 | 50.0 | 1 |
| Probolinggo | 05 | 71.0 | 1 |
| Probolinggo | 10 | 71.0 | 1 |
| Probolinggo | 07 | 71.0 | 1 |
| Surabaya | 10 | 67.0 | 1 |
| Surabaya | 05 | 62.0 | 1 |
| Surabaya | 07 | 62.0 | 1 |

### Jam Puncak Polusi per Kota

| Kota | Jam Puncak | Sesi | AQI Puncak |
|------|-----------|------|------------|
| Banyuwangi | 05.00 | Dini Hari (00–06) | 32.0 |
| Bojonegoro | 10.00 | Siang (10–16) | 57.0 |
| Jombang | 10.00 | Siang (10–16) | 51.0 |
| Lumajang | 05.00 | Dini Hari (00–06) | 25.0 |
| Madiun | 05.00 | Dini Hari (00–06) | 66.0 |
| Malang | 10.00 | Siang (10–16) | 54.0 |
| Mojokerto | 05.00 | Dini Hari (00–06) | 56.0 |
| Pasuruan | 10.00 | Siang (10–16) | 57.0 |
| Probolinggo | 05.00 | Dini Hari (00–06) | 71.0 |
| Surabaya | 10.00 | Siang (10–16) | 67.0 |

---

## Kesimpulan & Rekomendasi

### Temuan Utama

Tidak ditemukan pola jam sibuk pagi (07–09) maupun sore (17–19) pada data ini. Dua pola dominan yang muncul:

| Pola | Kota | Dugaan Penyebab |
|------|------|-----------------|
| **Dini Hari (jam 05)** | Probolinggo, Madiun, Mojokerto, Banyuwangi, Lumajang | Aktivitas industri / pembakaran sebelum subuh |
| **Siang (jam 10)** | Surabaya, Bojonegoro, Pasuruan, Malang, Jombang | Akumulasi polutan pagi + efek panas matahari |

### Rekomendasi untuk Dinas Kesehatan

1. **Notifikasi proaktif** — Kirim peringatan ISPU 30 menit sebelum jam puncak spesifik tiap kota.
2. **Jadwal aktivitas luar ruang** — Rekomendasikan olahraga setelah jam 08.00 untuk kota dengan puncak dini hari; hindari siang hari untuk kota dengan puncak jam 10.
3. **Koordinasi industri** — Dorong pembatasan aktivitas pembakaran industri di malam dan dini hari, khususnya di Probolinggo dan Madiun.
4. **Penguatan pemantauan** — Prioritaskan pengukuran lebih sering di Probolinggo (AQI 71) dan Surabaya (AQI 67) sebagai kota dengan polusi tertinggi.
5. **Edukasi berbasis lokal** — Jadwal penyuluhan dan peringatan harus disesuaikan per kota, bukan satu jadwal nasional.

---

## Analisis 3: Ranking Kota AQI Terburuk

Mengurutkan kota berdasarkan rata-rata AQI plus jumlah event "Tidak Sehat" (AQI > 100). Window function `rank()` digunakan untuk menentukan peringkat.

### Ranking Kota (Snapshot)

| Peringkat | Kota | Avg AQI | Max AQI | Min AQI | Event Tidak Sehat | Total Data |
|-----------|------|---------|---------|---------|-------------------|------------|
| 1 | Probolinggo | 71.0 | 71 | 71 | 0 | 3 |
| 2 | Madiun      | 66.0 | 66 | 66 | 0 | 3 |
| 3 | Surabaya    | 63.7 | 67 | 62 | 0 | 3 |
| 4 | Mojokerto   | 55.3 | 56 | 54 | 0 | 3 |
| 5 | Bojonegoro  | 54.0 | 57 | 52 | 0 | 3 |
| 6 | Pasuruan    | 52.3 | 57 | 50 | 0 | 3 |
| 7 | Malang      | 46.7 | 54 | 42 | 0 | 3 |
| 8 | Jombang     | 45.7 | 51 | 41 | 0 | 3 |
| 9 | Banyuwangi  | 29.7 | 32 | 25 | 0 | 3 |
| 10 | Lumajang   | 25.0 | 25 | 25 | 0 | 3 |

### Insight Ranking

- **Probolinggo, Madiun, Surabaya** menempati 3 besar — kandidat prioritas intervensi struktural Dinas Kesehatan.
- **Lumajang & Banyuwangi** konsisten di kategori Baik — bisa dijadikan benchmark kota target.
- Kolom `event_tidak_sehat = 0` di semua kota wajar untuk Jatim periode ini; kolom ini akan jadi indikator penting begitu producer berjalan kontinu di musim kemarau.

---

## Bonus AQI-B2: Prediksi AQI dengan Spark MLlib

Model **Linear Regression** dengan fitur:

- `jam` (numerik 0-23)
- `kota` (StringIndexer + OneHotEncoder)

Target: `aqi`. Split 80:20 dengan `seed=42`.

Output model (5 kota × 24 jam = 120 baris prediksi) ditambahkan ke `spark_results.json` dengan key `prediksi_aqi`. Dashboard A5 akan menampilkan kurva prediksi sebagai garis putus-putus melengkapi data live.

> Akurasi terbatas pada snapshot kecil saat ini — RMSE & R² dicatat di key `model_metrics`. Begitu data historis bertambah dan kita menambah fitur cuaca/lag AQI, ganti model ke `GBTRegressor` untuk performa lebih baik.

---

## Output Gabungan untuk Dashboard

File `dashboard/data/spark_results.json` berisi:

- `distribusi_kategori` (Analisis 1)
- `aqi_per_jam` + `jam_puncak_per_kota` (Analisis 2)
- `ranking_kota` (Analisis 3)
- `prediksi_aqi` + `model_metrics` (bonus MLlib)
- `generated_at`

Format lengkap dijelaskan di [`spark_results_schema.md`](./spark_results_schema.md). Sample mock di [`spark_results.example.json`](./spark_results.example.json) untuk testing dashboard tanpa harus menjalankan Spark.

---

### ⚠️ Catatan Validitas Data

Data saat ini masih terbatas — setiap slot jam hanya memiliki **1 data point** dan hanya mencakup **3 waktu pengukuran** (jam 05, 07, 10). Untuk kesimpulan yang valid secara statistik, producer perlu dijalankan terjadwal setiap 1 jam selama minimal 7–14 hari.