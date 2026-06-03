"""
02_silver.py — Silver Layer
============================
Tugas ini: bersihkan dan standarisasi data Bronze, simpan ke Silver Delta.

Kenapa Silver layer penting?
- Di ETS, Spark analisis langsung dari JSON mentah. Tipe data belum tentu benar
  (AQI bisa berupa string), ada duplikat, nama kota tidak konsisten.
  Semua analisis downstream ikut menanggung "noise" ini.
- Silver = satu sumber kebenaran yang sudah bersih. Semua analisis Gold
  dan dashboard membaca dari Silver, tidak perlu cleaning ulang.

Transformasi yang dilakukan (minimal 3, sesuai rubrik):
  T1 — Cast tipe data     : aqi, pm25 → double; ingested_at → timestamp
  T2 — Standarisasi kota  : trim + upper → "jakarta " == "JAKARTA"
  T3 — Filter nilai null  : baris tanpa kota atau AQI dibuang
  T4 — Filter anomali     : AQI di luar range 0-999 dibuang
  T5 — Deduplikasi        : kota + ingested_at yang sama hanya diambil sekali
  T6 — Ekstrak jam        : kolom "jam" (0-23) dari timestamp untuk analisis temporal
  T7 — Kategorisasi AQI   : kolom "kategori_aqi" berdasarkan skala ISPU Indonesia

Cara menjalankan:
  docker exec scheduler python3 /app/lakehouse/02_silver.py
"""

import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, hour, to_timestamp, trim, upper, when
)

# ── Path konfigurasi ──────────────────────────────────────────────────────────
DELTA_BASE    = os.getenv("DELTA_BASE_PATH", "/app/delta_lake")
BRONZE_API    = DELTA_BASE + "/bronze/airquality_api"
SILVER_PATH   = DELTA_BASE + "/silver/airquality"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("Silver-AirQuality")
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


def clean_api_data(spark):
    """
    Tujuh transformasi cleaning dengan justifikasi masing-masing.
    """
    print("\n[SILVER] Membaca Bronze API...")
    df_bronze = spark.read.format("delta").load(BRONZE_API)
    before_count = df_bronze.count()
    print(f"[SILVER] Bronze: {before_count} baris masuk")

    # ── T1: Cast tipe data ────────────────────────────────────────────────────
    # JSON tidak punya tipe data — semua bisa terbaca sebagai string.
    # Kalau aqi tetap string, AVG/MAX/MIN akan error atau salah.
    df = (
        df_bronze
        .withColumn("aqi",  col("aqi").cast("double"))
        .withColumn("pm25", col("pm25").cast("double"))
        # ingested_at dari API sudah format ISO8601 — parse ke TimestampType
        # agar bisa dipakai untuk Window Function (lag, rolling avg) di Gold
        .withColumn("ts", to_timestamp(col("ingested_at"),
                                       "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
    )

    # ── T2: Standarisasi nama kota ────────────────────────────────────────────
    # Data dari API ISPU kadang punya spasi ekstra atau case berbeda.
    # "jakarta " dan "JAKARTA" harus dianggap kota yang sama untuk groupBy.
    df = df.withColumn("kota", upper(trim(col("kota"))))

    # ── T3: Filter baris tanpa data kritis ───────────────────────────────────
    # Baris tanpa kota tidak bisa dianalisis per kota.
    # Baris tanpa AQI tidak bisa diklasifikasikan atau dirata-rata.
    before_null = df.count()
    df = df.filter(col("kota").isNotNull() & col("aqi").isNotNull())
    after_null = df.count()
    print(f"[SILVER] T3 (filter null): buang {before_null - after_null} baris")

    # ── T4: Filter nilai AQI anomali ─────────────────────────────────────────
    # AQI tidak mungkin negatif atau di atas 999 secara fisik.
    # Nilai ini bisa muncul dari error parsing atau glitch API.
    before_anomali = df.count()
    df = df.filter(col("aqi").between(0, 999))
    after_anomali = df.count()
    print(f"[SILVER] T4 (filter anomali): buang {before_anomali - after_anomali} baris")

    # ── T5: Deduplikasi ───────────────────────────────────────────────────────
    # Consumer Kafka kadang kirim ulang event yang sama (at-least-once delivery).
    # Kombinasi kota + ingested_at harus unik — kalau ada duplikat, ambil satu.
    before_dedup = df.count()
    df = df.dropDuplicates(["kota", "ingested_at"])
    after_dedup = df.count()
    print(f"[SILVER] T5 (dedup): buang {before_dedup - after_dedup} duplikat")

    # ── T6: Ekstrak kolom jam ─────────────────────────────────────────────────
    # Kolom "jam" (0-23) diekstrak dari timestamp agar Gold bisa groupBy jam
    # tanpa perlu parse ulang. Di ETS ini dilakukan inline di analysis.py —
    # sekarang dilakukan sekali di Silver untuk semua analisis.
    df = df.withColumn("jam", hour(col("ts")))

    # ── T7: Kategorisasi AQI (skala ISPU Indonesia) ───────────────────────────
    # Dipindahkan dari analysis.py ke Silver agar kategorisasi konsisten
    # di semua tabel Gold. Tidak perlu mendefinisikan ulang skala di tiap script.
    df = df.withColumn(
        "kategori_aqi",
        when(col("aqi") <= 50,  "Baik")
        .when(col("aqi") <= 100, "Sedang")
        .when(col("aqi") <= 200, "Tidak Sehat")
        .otherwise("Berbahaya"),
    )

    df = df.withColumn("_processed_at", current_timestamp())

    after_count = df.count()
    print(f"[SILVER] Total: {before_count} → {after_count} baris "
          f"({before_count - after_count} dibuang)")

    return df


def main():
    spark = create_spark()
    try:
        print("=" * 60)
        print("SILVER LAYER — AirQuality Alert")
        print("=" * 60)

        df_silver = clean_api_data(spark)

        print("\n[SILVER] Schema output:")
        df_silver.printSchema()

        print("[SILVER] Sample 5 baris:")
        df_silver.select(
            "kota", "aqi", "pm25", "kategori_aqi", "jam", "ts", "_processed_at"
        ).show(5, truncate=False)

        # Statistik per kota untuk dokumentasi README
        print("[SILVER] Ringkasan per kota:")
        df_silver.groupBy("kota", "kategori_aqi").count().orderBy("kota").show()

        # Simpan ke Silver Delta
        # mode("overwrite") → Silver selalu up-to-date dari Bronze terbaru
        (
            df_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_PATH)
        )
        print(f"\n[SILVER] Selesai → {SILVER_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
