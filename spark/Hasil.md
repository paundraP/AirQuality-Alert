## Hasil Analisis & Kesimpulan

Analisis ini mencakup tiga bagian wajib (AQI-07, AQI-08, AQI-09) plus prediksi MLlib bonus (AQI-B2). Output gabungan disimpan ke `dashboard/data/spark_results.json` dan dikonsumsi oleh dashboard A5 melalui endpoint `/api/spark`.

### Analisis 1: Distribusi Kategori AQI per Kota

Dari data yang terkumpul, semua kota di Jawa Timur masuk kategori **Baik** atau **Sedang** — tidak ada yang Tidak Sehat atau Berbahaya. Kota dengan AQI tertinggi adalah **Probolinggo (71)** dan **Madiun (66)**, keduanya konsisten Sedang di semua waktu pengukuran.

---

### Analisis 2: Jam Puncak Polusi

| Kota | Jam Puncak | Sesi | AQI Puncak |
|------|-----------|------|------------|
| Probolinggo | 05.00 | Dini Hari | 71.0 |
| Surabaya | 10.00 | Siang | 67.0 |
| Madiun | 05.00 | Dini Hari | 66.0 |
| Bojonegoro | 10.00 | Siang | 57.0 |
| Pasuruan | 10.00 | Siang | 57.0 |
| Mojokerto | 05.00 | Dini Hari | 56.0 |
| Malang | 10.00 | Siang | 54.0 |
| Jombang | 10.00 | Siang | 51.0 |
| Banyuwangi | 05.00 | Dini Hari | 32.0 |
| Lumajang | 05.00 | Dini Hari | 25.0 |

---

### Kesimpulan

**Tidak ada kota yang puncaknya di jam sibuk pagi (07-09) maupun sore (17-19).** Ada dua pola yang muncul:

**Pola Dini Hari (jam 5)** — Probolinggo, Madiun, Mojokerto, Banyuwangi, Lumajang. Ini mengindikasikan aktivitas industri atau pembakaran yang terjadi sebelum subuh, bukan dari kendaraan.

**Pola Siang (jam 10)** — Surabaya, Bojonegoro, Pasuruan, Malang, Jombang. Kemungkinan disebabkan akumulasi polutan dari aktivitas pagi yang memuncak setelah beberapa jam, ditambah efek panas matahari yang memperburuk dispersi polutan.

---

### Analisis 3: Ranking Kota AQI Terburuk

Ranking dihitung dari rata-rata AQI tiap kota plus jumlah event dengan AQI > 100 ("Tidak Sehat"). Window function `rank()` dipakai untuk menentukan peringkat 1 sampai N.

Dengan snapshot data saat ini, urutan teratas didominasi **Probolinggo (avg ~71)**, **Surabaya (avg ~63-67)**, dan **Madiun (avg ~66)**. Belum ada kota dengan event AQI > 100, sehingga `event_tidak_sehat = 0` di semua baris — wajar untuk data Jatim yang umumnya masih Baik/Sedang. Begitu producer berjalan kontinu di musim kemarau atau di lokasi industri padat, kolom ini akan membantu memprioritaskan kota dengan lonjakan polusi episodik.

**Rekomendasi prioritas Dinas Kesehatan**:

1. Top 3 kota (Probolinggo, Surabaya, Madiun) menjadi fokus program penurunan emisi struktural.
2. Kota dengan `event_tidak_sehat` > 0 mendapat prioritas pemasangan sensor real-time tambahan.
3. Sosialisasi penggunaan masker N95 dan jadwal aktivitas luar ruang difokuskan ke 3 kota teratas.

---

### Bonus MLlib (AQI-B2): Prediksi AQI per Jam

Model **LinearRegression** dilatih dengan fitur `jam` (0-23) dan `kota` (one-hot). Output prediksi (5 kota × 24 jam) disimpan ke key `prediksi_aqi` di `spark_results.json` agar dashboard bisa menampilkan kurva prediksi 24 jam.

> Akurasi model masih terbatas (R² rendah) selama data historis di HDFS belum cukup banyak — hanya kerangka pipeline ML yang divalidasi. Setelah producer berjalan beberapa hari, retrain dengan menambahkan fitur cuaca dan lag AQI akan meningkatkan akurasi secara signifikan.

---

### ⚠️ Catatan Penting: Data Masih Terlalu Sedikit

Hasil ini **belum bisa dijadikan kesimpulan final** karena:

- Setiap jam hanya punya **1 data point** (`jumlah_data = 1`) — idealnya puluhan hingga ratusan
- Data hanya dari **3 waktu pengukuran** (jam 05, 07, 10) — belum mencakup 24 jam penuh
- Producer hanya jalan beberapa kali, belum berjalan **kontinu sepanjang hari**

Untuk hasil yang valid, producer perlu dijalankan secara terjadwal (misal setiap 1 jam) selama minimal beberapa hari agar setiap slot jam punya cukup data untuk dirata-rata secara bermakna.