# `spark_results.json` — Kontrak Format A4 ↔ A5

File ini adalah **kontrak format** antara A4 (Spark) dan A5 (Dashboard). File aktual di-generate oleh `spark/analysis.ipynb` (cell terakhir AQI-10) ke path `dashboard/data/spark_results.json`. Dashboard membacanya via endpoint `GET /api/spark`.

> **Catatan:** path `dashboard/data/` ada di `.gitignore`, jadi file ini tidak ikut di-push. A5 cukup menjalankan notebook A4 sekali untuk menghasilkan file lokal sebelum testing.

## Struktur Top-Level

```json
{
  "distribusi_kategori":  [ ... ],
  "aqi_per_jam":          [ ... ],
  "ranking_kota":         [ ... ],
  "jam_puncak_per_kota":  [ ... ],
  "prediksi_aqi":         [ ... ],   // bonus, dari MLlib
  "model_metrics":        { ... },   // bonus, dari MLlib
  "generated_at":         "2026-05-04T16:30:00+07:00"
}
```

## Detail per Field

### 1. `distribusi_kategori` (dari Analisis 1 — AQI-07)

Dipakai dashboard untuk pewarnaan baris ranking dan pie/stacked-bar opsional.

```json
[
  { "kota": "surabaya",    "kategori": "Sedang", "persentase": 100.0 },
  { "kota": "probolinggo", "kategori": "Sedang", "persentase": 100.0 },
  { "kota": "jombang",     "kategori": "Baik",   "persentase": 66.7  },
  { "kota": "jombang",     "kategori": "Sedang", "persentase": 33.3  }
]
```

| Field        | Tipe     | Keterangan                                              |
|--------------|----------|---------------------------------------------------------|
| `kota`       | string   | nama kota (lowercase, tanpa prefix "kota"/"kabupaten") |
| `kategori`   | string   | salah satu: `Baik` / `Sedang` / `Tidak Sehat` / `Berbahaya` |
| `persentase` | float    | persen kemunculan kategori di kota tsb (0-100)         |

### 2. `aqi_per_jam` (dari Analisis 2 — AQI-08)

Dipakai dashboard untuk line chart "AQI per Jam" (P3 bonus / Chart.js).

```json
[
  { "kota": "surabaya", "jam": 10, "avg_aqi": 67.0, "jumlah_data": 1 },
  { "kota": "surabaya", "jam": 7,  "avg_aqi": 62.0, "jumlah_data": 1 }
]
```

| Field         | Tipe   | Keterangan                                  |
|---------------|--------|---------------------------------------------|
| `kota`        | string | nama kota                                   |
| `jam`         | int    | jam dalam sehari, 0-23                      |
| `avg_aqi`     | float  | rata-rata AQI di jam tsb                    |
| `jumlah_data` | int    | jumlah event yang dirata-rata               |

### 3. `ranking_kota` (dari Analisis 3 — AQI-09) — **panel utama dashboard**

```json
[
  {
    "peringkat": 1,
    "kota": "probolinggo",
    "avg_aqi": 71.0,
    "max_aqi": 71,
    "min_aqi": 71,
    "event_tidak_sehat": 0,
    "total_data": 3
  }
]
```

| Field               | Tipe   | Keterangan                                  |
|---------------------|--------|---------------------------------------------|
| `peringkat`         | int    | rank, 1 = AQI rata-rata tertinggi (terburuk)|
| `kota`              | string | nama kota                                   |
| `avg_aqi`           | float  | rata-rata AQI seluruh data                  |
| `max_aqi`           | int    | AQI maksimum                                |
| `min_aqi`           | int    | AQI minimum                                 |
| `event_tidak_sehat` | int    | jumlah event dengan `aqi > 100`             |
| `total_data`        | int    | total event di kota tsb                     |

**Pewarnaan baris di dashboard** (P1) mengikuti `avg_aqi`:

| Range `avg_aqi` | Kategori     | Warna   |
|-----------------|--------------|---------|
| 0 – 50          | Baik         | Hijau   |
| 51 – 100        | Sedang       | Kuning  |
| 101 – 200       | Tidak Sehat  | Oranye  |
| > 200           | Berbahaya    | Merah   |

### 4. `jam_puncak_per_kota` (dari Analisis 2 — AQI-08, ringkasan)

```json
[
  {
    "kota": "surabaya",
    "jam_puncak": 10,
    "avg_aqi_puncak": 67.0,
    "jumlah_data_puncak": 1,
    "sesi": "Siang (10-16)"
  }
]
```

| Field                | Tipe   | Keterangan                                    |
|----------------------|--------|-----------------------------------------------|
| `kota`               | string | nama kota                                     |
| `jam_puncak`         | int    | jam (0-23) dengan rata-rata AQI tertinggi     |
| `avg_aqi_puncak`     | float  | nilai AQI rata-rata di jam puncak             |
| `jumlah_data_puncak` | int    | jumlah event di jam puncak                    |
| `sesi`               | string | label sesi: `Pagi Sibuk (07-09)` / `Sore Sibuk (17-19)` / `Siang (10-16)` / `Malam (20-23)` / `Dini Hari (00-06)` |

### 5. `prediksi_aqi` *(bonus AQI-B2 — MLlib)*

```json
[
  { "kota": "surabaya", "jam": 0,  "predicted_aqi": 58.3 },
  { "kota": "surabaya", "jam": 1,  "predicted_aqi": 57.9 }
]
```

5 kota × 24 jam = 120 row. Dashboard bisa menampilkan kurva prediksi sebagai garis putus-putus di line chart `aqi_per_jam`.

### 6. `model_metrics` *(bonus AQI-B2)*

```json
{
  "algorithm":  "LinearRegression",
  "features":   ["jam", "kota (one-hot)"],
  "train_rows": 24,
  "test_rows":  6,
  "rmse":       12.34,
  "r2":         0.18
}
```

Dipakai untuk menampilkan badge "Model: LinearRegression — RMSE 12.34 / R² 0.18" di pojok dashboard.

### 7. `generated_at`

ISO-8601 string dengan offset WIB, contoh: `2026-05-04T16:30:00+07:00`. Dashboard bisa menampilkan ini sebagai "Spark results updated at HH:MM:SS".

## Cara Refresh File

```bash
# Dari root repo
cd spark
jupyter nbconvert --to notebook --execute analysis.ipynb --inplace
```

Setelah perintah di atas, file `dashboard/data/spark_results.json` akan ter-overwrite dengan hasil terbaru dari HDFS.
