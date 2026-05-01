## Hasil Analisis & Kesimpulan

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

### ⚠️ Catatan Penting: Data Masih Terlalu Sedikit

Hasil ini **belum bisa dijadikan kesimpulan final** karena:

- Setiap jam hanya punya **1 data point** (`jumlah_data = 1`) — idealnya puluhan hingga ratusan
- Data hanya dari **3 waktu pengukuran** (jam 05, 07, 10) — belum mencakup 24 jam penuh
- Producer hanya jalan beberapa kali, belum berjalan **kontinu sepanjang hari**

Untuk hasil yang valid, producer perlu dijalankan secara terjadwal (misal setiap 1 jam) selama minimal beberapa hari agar setiap slot jam punya cukup data untuk dirata-rata secara bermakna.