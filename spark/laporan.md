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

### ⚠️ Catatan Validitas Data

Data saat ini masih terbatas — setiap slot jam hanya memiliki **1 data point** dan hanya mencakup **3 waktu pengukuran** (jam 05, 07, 10). Untuk kesimpulan yang valid secara statistik, producer perlu dijalankan terjadwal setiap 1 jam selama minimal 7–14 hari.