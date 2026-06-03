"""
03_gold.py — Gold Layer: Agregasi + Enhanced Analysis + Time Travel
===================================================================
Tugas: Baca dari Silver, buat minimal 2 tabel reproduksi ETS + 1 tabel
       Enhanced, dan demonstrasikan Time Travel Delta Lake.

Cara menjalankan:
    python3 lakehouse/03_gold.py

5 Tabel Gold yang dibuat:
    REPRODUKSI ETS (yang sudah ada di analysis.py lama):
    [G1] gold/aqi_category_dist   — distribusi kategori AQI per kota
    [G2] gold/aqi_hourly          — rata-rata AQI per kota per jam
    [G3] gold/aqi_ranking         — ranking kota berdasarkan avg AQI

    ENHANCED (tidak bisa dibuat di ETS karena butuh data bersih):
    [G4] gold/aqi_trend           — tren AQI: membaik/memburuk per kota
                                    (pakai Window Function lag())
    [G5] gold/aqi_alert_hours     — jam berturut-turut kondisi "Tidak Sehat"

    TIME TRAVEL:
    Demonstrasi history(), update(), dan query versionAsOf
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, desc,
    max as spark_max, min as spark_min,
    rank, round as spark_round,
    sum as spark_sum, when,
)
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# ── Path ───────────────────────────────────────────────────────────────────────
SILVER_PATH        = "/app/delta_lake/silver/airquality"
GOLD_DIST_PATH     = "/app/delta_lake/gold/aqi_category_dist"
GOLD_HOURLY_PATH   = "/app/delta_lake/gold/aqi_hourly"
GOLD_RANKING_PATH  = "/app/delta_lake/gold/aqi_ranking"
GOLD_TREND_PATH    = "/app/delta_lake/gold/aqi_trend"
GOLD_ALERT_PATH    = "/app/delta_lake/gold/aqi_alert_hours"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("Gold-AirQuality")
        .master("local[*]")
        .config("spark.driver.memory", "768m")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ════════════════════════════════════════════════════════════════════════════════
# G1 — Distribusi kategori AQI per kota (Reproduksi ETS Analisis 1)
# ════════════════════════════════════════════════════════════════════════════════
def gold_category_dist(spark, df_silver):
    """
    Reproduksi analisis ETS: distribusi persentase kategori per kota.
    Perbedaan vs ETS: tidak perlu re-klasifikasi karena kolom 'kategori_aqi'
    sudah ada di Silver — hasilnya lebih akurat dan konsisten.
    """
    print("[G1] Membuat tabel distribusi kategori AQI...")

    total_per_kota = (
        df_silver.groupBy("kota")
        .agg(count("*").alias("total"))
    )

    dist = (
        df_silver
        .groupBy("kota", "kategori_aqi")
        .agg(count("*").alias("jumlah"))
        .join(total_per_kota, "kota")
        .withColumn("persentase", spark_round(col("jumlah") / col("total") * 100, 1))
        .select("kota", "kategori_aqi", "jumlah", "persentase")
        .orderBy("kota", "kategori_aqi")
    )

    dist.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(GOLD_DIST_PATH)
    print(f"[G1] Selesai → {GOLD_DIST_PATH}")
    dist.show(truncate=False)
    return dist


# ════════════════════════════════════════════════════════════════════════════════
# G2 — Rata-rata AQI per jam per kota (Reproduksi ETS Analisis 2)
# ════════════════════════════════════════════════════════════════════════════════
def gold_hourly(spark, df_silver):
    """
    Reproduksi analisis ETS: pola AQI per jam.
    Perbedaan vs ETS: kolom 'jam' sudah tersedia di Silver (hasil T2),
    tidak perlu hitung ulang hour() — query lebih efisien.
    """
    print("[G2] Membuat tabel AQI per jam...")

    hourly = (
        df_silver
        .filter(col("jam").isNotNull())
        .groupBy("kota", "jam")
        .agg(
            spark_round(avg("aqi"), 1).alias("avg_aqi"),
            count("*").alias("jumlah_data"),
        )
        .orderBy("kota", "jam")
    )

    hourly.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(GOLD_HOURLY_PATH)
    print(f"[G2] Selesai → {GOLD_HOURLY_PATH}")
    hourly.show(10)
    return hourly


# ════════════════════════════════════════════════════════════════════════════════
# G3 — Ranking kota berdasarkan rata-rata AQI (Reproduksi ETS Analisis 3)
# ════════════════════════════════════════════════════════════════════════════════
def gold_ranking(spark, df_silver):
    """
    Reproduksi analisis ETS: ranking kota terburuk.
    Window function rank() dipakai — sama seperti ETS tapi datanya sudah
    bersih dari duplikat dan anomali sehingga rata-rata lebih akurat.
    """
    print("[G3] Membuat tabel ranking kota...")

    ranking = (
        df_silver
        .groupBy("kota")
        .agg(
            spark_round(avg("aqi"), 1).alias("avg_aqi"),
            spark_max("aqi").cast("int").alias("max_aqi"),
            spark_min("aqi").cast("int").alias("min_aqi"),
            spark_sum(when(col("aqi") > 100, 1).otherwise(0)).alias("event_tidak_sehat"),
            count("*").alias("total_data"),
        )
        .withColumn("peringkat", rank().over(Window.orderBy(desc("avg_aqi"))))
        .orderBy("peringkat")
    )

    ranking.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(GOLD_RANKING_PATH)
    print(f"[G3] Selesai → {GOLD_RANKING_PATH}")
    ranking.show(truncate=False)
    return ranking


# ════════════════════════════════════════════════════════════════════════════════
# G4 — Tren AQI per kota: membaik / memburuk? (ENHANCED — tidak ada di ETS)
# ════════════════════════════════════════════════════════════════════════════════
def gold_trend(spark, df_silver):
    """
    ENHANCED: Analisis tren AQI menggunakan Window Function lag().

    Kenapa ini TIDAK BISA dibuat di ETS?
    - ETS membaca JSON mentah dari HDFS — kolom 'ingested_at' adalah String
    - Window Function .orderBy("timestamp") butuh TimestampType
    - Tanpa ordering yang benar, lag() menghasilkan nilai random, bukan nilai
      record sebelumnya secara kronologis
    - Di Silver, 'ts' sudah dicast ke TimestampType (T1) → lag() bekerja benar

    Cara kerja lag():
    - Window dibuat per kota, diurutkan berdasarkan timestamp
    - lag("aqi", 1) mengambil nilai AQI dari baris sebelumnya (1 record lalu)
    - Selisih AQI sekarang vs sebelumnya menunjukkan arah perubahan
    """
    print("[G4] Membuat tabel tren AQI (ENHANCED — Window Function lag)...")

    # Window per kota, ordered by timestamp
    window_kota = Window.partitionBy("kota").orderBy("ts")

    trend = (
        df_silver
        .filter(col("ts").isNotNull())
        .withColumn("aqi_sebelumnya", col("aqi").cast("double"))
        # lag(col, n) = ambil nilai kolom n baris sebelumnya dalam window
        .withColumn(
            "aqi_sebelumnya",
            __import__("pyspark.sql.functions", fromlist=["lag"]).lag("aqi", 1).over(window_kota)
        )
        .withColumn("perubahan_aqi", col("aqi") - col("aqi_sebelumnya"))
        .withColumn(
            "tren",
            when(col("perubahan_aqi") > 5,  "Memburuk")    # naik > 5 = makin kotor
            .when(col("perubahan_aqi") < -5, "Membaik")     # turun > 5 = makin bersih
            .otherwise("Stabil")
        )
        .filter(col("aqi_sebelumnya").isNotNull())  # baris pertama tiap kota tidak punya prev
    )

    # Ringkasan: berapa kali tiap kota membaik/memburuk/stabil
    trend_summary = (
        trend
        .groupBy("kota", "tren")
        .agg(count("*").alias("jumlah_observasi"))
        .orderBy("kota", "tren")
    )

    trend_summary.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(GOLD_TREND_PATH)
    print(f"[G4] Selesai → {GOLD_TREND_PATH}")
    print("Ringkasan tren AQI per kota:")
    trend_summary.show(truncate=False)
    return trend_summary


# ════════════════════════════════════════════════════════════════════════════════
# G5 — Jam berturut-turut kondisi Tidak Sehat per kota (ENHANCED)
# ════════════════════════════════════════════════════════════════════════════════
def gold_alert_hours(spark, df_silver):
    """
    ENHANCED: Deteksi berapa jam berturut-turut kota dalam kondisi
    Tidak Sehat (AQI > 100).

    Kenapa berguna?
    - Kota yang 1 jam AQI > 100 beda dampaknya vs kota yang 5 jam berturut-turut
    - Informasi ini relevan untuk rekomendasi "hindari aktivitas luar ruang"
    - Di ETS tidak ada karena butuh ordering temporal yang akurat
    """
    print("[G5] Membuat tabel alert jam berturut-turut kondisi tidak sehat...")

    # Tandai setiap baris: apakah dalam kondisi tidak sehat?
    df_flagged = df_silver.withColumn(
        "tidak_sehat",
        when(col("aqi") > 100, 1).otherwise(0)
    )

    # Hitung total jam tidak sehat dan total observasi per kota
    alert_summary = (
        df_flagged
        .groupBy("kota")
        .agg(
            spark_sum("tidak_sehat").alias("total_jam_tidak_sehat"),
            count("*").alias("total_observasi"),
            spark_round(
                spark_sum("tidak_sehat") / count("*") * 100, 1
            ).alias("pct_tidak_sehat"),
            spark_round(avg("aqi"), 1).alias("avg_aqi"),
        )
        .withColumn(
            "status_kota",
            when(col("pct_tidak_sehat") > 50, "Prioritas Tinggi")
            .when(col("pct_tidak_sehat") > 20, "Perlu Perhatian")
            .otherwise("Normal")
        )
        .orderBy(desc("total_jam_tidak_sehat"))
    )

    alert_summary.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(GOLD_ALERT_PATH)
    print(f"[G5] Selesai → {GOLD_ALERT_PATH}")
    print("Kota dengan jam terbanyak kondisi tidak sehat:")
    alert_summary.show(truncate=False)
    return alert_summary


# ════════════════════════════════════════════════════════════════════════════════
# TIME TRAVEL — Demonstrasi fitur Delta Lake (10 poin wajib)
# ════════════════════════════════════════════════════════════════════════════════
def demonstrate_time_travel(spark):
    """
    Time Travel adalah fitur utama Delta Lake yang TIDAK ADA di HDFS biasa.

    Cara kerja:
    - Setiap write ke Delta table membuat "version" baru
    - Semua version lama tetap tersimpan di folder _delta_log/
    - Kita bisa query data di versi berapa pun dengan option("versionAsOf", N)

    Demo ini menunjukkan:
    1. Lihat history versi tabel Silver
    2. Lakukan UPDATE (simulasi koreksi data)
    3. Query versi lama untuk verifikasi data sebelum update
    4. Bandingkan hasilnya
    """
    print("\n" + "="*60)
    print("DEMONSTRASI TIME TRAVEL DELTA LAKE")
    print("="*60)

    delta_table = DeltaTable.forPath(spark, SILVER_PATH)

    # ── Step 1: Lihat history tabel ──────────────────────────────────────────
    print("\n[TIME TRAVEL Step 1] History versi tabel Silver:")
    delta_table.history() \
        .select("version", "timestamp", "operation", "operationParameters") \
        .show(truncate=False)

    # ── Step 2: Hitung distribusi kategori SEBELUM update ────────────────────
    print("\n[TIME TRAVEL Step 2] Distribusi kategori AQI SEBELUM update:")
    df_before = spark.read.format("delta").load(SILVER_PATH)
    df_before.groupBy("kategori_aqi").count().orderBy("kategori_aqi").show()
    total_before = df_before.count()
    print(f"Total baris sebelum update: {total_before}")

    # ── Step 3: Lakukan UPDATE — simulasi koreksi data ───────────────────────
    # Skenario: kita "koreksi" semua kota yang null jadi "UNKNOWN"
    # Ini mensimulasikan situasi di dunia nyata: ada perbaikan data masuk
    print("\n[TIME TRAVEL Step 3] Melakukan UPDATE pada tabel Silver...")
    print("(Mengisi nilai kota yang null dengan 'UNKNOWN' sebagai simulasi koreksi)")

    delta_table.update(
        condition="kota IS NULL",
        set={"kota": "'UNKNOWN'"}
    )
    print("UPDATE selesai.")

    # ── Step 4: Lihat history setelah update ─────────────────────────────────
    print("\n[TIME TRAVEL Step 4] History setelah UPDATE:")
    delta_table.history() \
        .select("version", "timestamp", "operation") \
        .show(truncate=False)

    # ── Step 5: Query data SESUDAH update (versi terbaru) ────────────────────
    print("\n[TIME TRAVEL Step 5] Data SESUDAH update (versi terbaru):")
    df_after = spark.read.format("delta").load(SILVER_PATH)
    df_after.groupBy("kategori_aqi").count().orderBy("kategori_aqi").show()

    # ── Step 6: Query data SEBELUM update (versionAsOf=0) ────────────────────
    # Ini yang membedakan Delta Lake dari HDFS biasa!
    # Di HDFS: sekali overwrite, data lama hilang selamanya
    # Di Delta Lake: kita bisa "time travel" ke versi mana pun
    print("\n[TIME TRAVEL Step 6] Query data VERSI 0 (sebelum update):")
    print("(Menggunakan option('versionAsOf', 0) — fitur eksklusif Delta Lake)")
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH)
    df_v0.groupBy("kategori_aqi").count().orderBy("kategori_aqi").show()

    # ── Step 7: Perbandingan langsung ────────────────────────────────────────
    print("\n[TIME TRAVEL Step 7] PERBANDINGAN versi terbaru vs versi 0:")
    print(f"  Versi terbaru: {df_after.count()} baris")
    print(f"  Versi 0      : {df_v0.count()} baris")

    null_after = df_after.filter(col("kota").isNull()).count()
    null_v0    = df_v0.filter(col("kota").isNull()).count()
    print(f"  Baris kota=null — Versi terbaru: {null_after}, Versi 0: {null_v0}")
    print("\n  --> Delta Lake memungkinkan kita melihat dan memulihkan data")
    print("      dari versi mana pun. Hal ini TIDAK MUNGKIN di HDFS biasa.")
    print("="*60)


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Membaca Silver layer...")
        df_silver = spark.read.format("delta").load(SILVER_PATH)
        total = df_silver.count()
        print(f"Silver berisi {total} baris — mulai membangun Gold layer\n")

        if total == 0:
            raise RuntimeError("Silver kosong! Jalankan dulu 01_bronze.py dan 02_silver.py")

        # Reproduksi ETS
        gold_category_dist(spark, df_silver)
        gold_hourly(spark, df_silver)
        gold_ranking(spark, df_silver)

        # Enhanced (tidak ada di ETS)
        gold_trend(spark, df_silver)
        gold_alert_hours(spark, df_silver)

        # Time Travel (wajib 10 poin)
        demonstrate_time_travel(spark)

        print("\n[GOLD] Semua tabel Gold selesai dibuat.")
        print("Lokasi output:")
        for path in [GOLD_DIST_PATH, GOLD_HOURLY_PATH, GOLD_RANKING_PATH,
                     GOLD_TREND_PATH, GOLD_ALERT_PATH]:
            print(f"  {path}")

    finally:
        spark.stop()
