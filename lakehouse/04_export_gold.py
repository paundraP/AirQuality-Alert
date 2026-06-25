"""
04_export_gold.py — Export Gold Delta ke JSON untuk Dashboard
=============================================================
Tugas: Baca 3 tabel Gold (ranking, distribusi, hourly) lalu gabungkan
       menjadi gold_results.json yang dibaca oleh dashboard/app.py.

Cara menjalankan (setelah 03_gold.py):
    python3 lakehouse/04_export_gold.py

Output:
    dashboard/data/gold_results.json

Kenapa script terpisah?
- Dashboard Flask tidak punya PySpark — tidak bisa baca Delta langsung.
- Script ini menjadi "jembatan" antara Gold Delta dan Flask dashboard.
- Setiap kali pipeline Gold selesai, jalankan script ini untuk refresh data.
"""

import os
import json
from datetime import datetime, timezone, timedelta

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# ── Path konfigurasi ──────────────────────────────────────────────────────────
DELTA_BASE         = os.getenv("DELTA_BASE_PATH", "/app/delta_lake")
GOLD_RANKING_PATH  = DELTA_BASE + "/gold/aqi_ranking"
GOLD_DIST_PATH     = DELTA_BASE + "/gold/aqi_category_dist"
GOLD_HOURLY_PATH   = DELTA_BASE + "/gold/aqi_hourly"
GOLD_TREND_PATH    = DELTA_BASE + "/gold/aqi_trend"
GOLD_ALERT_PATH    = DELTA_BASE + "/gold/aqi_alert_hours"

OUTPUT_PATH = os.getenv(
    "GOLD_EXPORT_PATH",
    "/app/dashboard/data/gold_results.json"
)


def create_spark():
    builder = (
        SparkSession.builder
        .appName("ExportGold-AirQuality")
        .master("local[*]")
        .config("spark.driver.memory", "512m")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_gold(spark, path, label):
    """Baca Gold Delta table, return list of dict."""
    try:
        df = spark.read.format("delta").load(path)
        rows = [row.asDict() for row in df.collect()]
        print(f"[EXPORT] {label}: {len(rows)} baris")
        return rows
    except Exception as e:
        print(f"[EXPORT] WARNING: Tidak bisa baca {label} — {e}")
        return []


def main():
    spark = create_spark()
    try:
        print("=" * 60)
        print("EXPORT GOLD → dashboard/data/gold_results.json")
        print("=" * 60)

        # ── Baca semua tabel Gold ─────────────────────────────────────────────
        ranking   = read_gold(spark, GOLD_RANKING_PATH,  "aqi_ranking")
        distribusi = read_gold(spark, GOLD_DIST_PATH,    "aqi_category_dist")
        hourly    = read_gold(spark, GOLD_HOURLY_PATH,   "aqi_hourly")
        trend     = read_gold(spark, GOLD_TREND_PATH,    "aqi_trend")
        alert     = read_gold(spark, GOLD_ALERT_PATH,    "aqi_alert_hours")

        # ── Konversi tipe data agar JSON-serializable ─────────────────────────
        def to_serializable(obj):
            if hasattr(obj, 'item'):        # numpy types
                return obj.item()
            if hasattr(obj, 'isoformat'):   # datetime
                return obj.isoformat()
            return str(obj)

        def clean_rows(rows):
            cleaned = []
            for row in rows:
                cleaned.append({
                    k: (v if isinstance(v, (str, int, float, bool, type(None)))
                        else to_serializable(v))
                    for k, v in row.items()
                })
            return cleaned

        ranking    = clean_rows(ranking)
        distribusi = clean_rows(distribusi)
        hourly     = clean_rows(hourly)
        trend      = clean_rows(trend)
        alert      = clean_rows(alert)

        # ── Bangun payload yang kompatibel dengan app.py ──────────────────────
        # Format ranking_kota dipertahankan sama dengan spark_results.json lama
        # sehingga app.py tidak perlu banyak diubah.
        wib = timezone(timedelta(hours=7))
        payload = {
            "source": "gold_delta",          # penanda: data berasal dari Gold
            "generated_at": datetime.now(wib).isoformat(),

            # ── Kompatibel dengan app.py lama ─────────────────────────────────
            "ranking_kota": [
                {
                    "kota":              r.get("kota", ""),
                    "avg_aqi":           r.get("avg_aqi"),
                    "max_aqi":           r.get("max_aqi"),
                    "min_aqi":           r.get("min_aqi"),
                    "event_tidak_sehat": r.get("event_tidak_sehat"),
                    "total_data":        r.get("total_data"),
                    "peringkat":         r.get("peringkat"),
                }
                for r in ranking
            ],

            # ── Baru: distribusi kategori dari G1 ────────────────────────────
            "distribusi_kategori": [
                {
                    "kota":       d.get("kota", ""),
                    "kategori":   d.get("kategori_aqi", ""),
                    "jumlah_data": d.get("jumlah"),
                    "persentase": d.get("persentase"),
                }
                for d in distribusi
            ],

            # ── Baru: rata-rata AQI per jam dari G2 ──────────────────────────
            "aqi_per_jam": [
                {
                    "kota":        h.get("kota", ""),
                    "jam":         h.get("jam"),
                    "avg_aqi":     h.get("avg_aqi"),
                    "jumlah_data": h.get("jumlah_data"),
                }
                for h in hourly
            ],

            # ── Baru: tren AQI dari G4 (Enhanced) ────────────────────────────
            "aqi_trend": [
                {
                    "kota":             t.get("kota", ""),
                    "tren":             t.get("tren", ""),
                    "jumlah_observasi": t.get("jumlah_observasi"),
                }
                for t in trend
            ],

            # ── Baru: alert kota dari G5 (Enhanced) ──────────────────────────
            "aqi_alert": [
                {
                    "kota":                  a.get("kota", ""),
                    "total_jam_tidak_sehat": a.get("total_jam_tidak_sehat"),
                    "pct_tidak_sehat":       a.get("pct_tidak_sehat"),
                    "avg_aqi":               a.get("avg_aqi"),
                    "status_kota":           a.get("status_kota", ""),
                }
                for a in alert
            ],
        }

        # ── Tulis ke file ─────────────────────────────────────────────────────
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

        print(f"\n[EXPORT] Selesai → {OUTPUT_PATH}")
        print(f"  ranking_kota     : {len(payload['ranking_kota'])} kota")
        print(f"  distribusi_kategori: {len(payload['distribusi_kategori'])} baris")
        print(f"  aqi_per_jam      : {len(payload['aqi_per_jam'])} baris")
        print(f"  aqi_trend        : {len(payload['aqi_trend'])} baris")
        print(f"  aqi_alert        : {len(payload['aqi_alert'])} kota")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()