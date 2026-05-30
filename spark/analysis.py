"""
analysis.py — Pipeline Medallion untuk AirQuality-Alert
========================================================
Arsitektur:
  HDFS (JSON raw)
    → BRONZE  : raw data + metadata ingest, schema enforcement
    → SILVER  : cleaned, dedup, cast tipe, kategorisasi AQI
    → GOLD    : agregasi untuk dashboard (ranking, jam puncak, distribusi)
    → EXPORT  : Gold → spark_results.json untuk Flask dashboard

Semua analisis lama (distribusi, jam puncak, ranking, MLlib) tetap ada,
hanya sekarang dibaca dari SILVER (data bersih) bukan langsung dari HDFS.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    desc,
    first,
    hour,
    lit,
    max as spark_max,
    min as spark_min,
    rank,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.window import Window


# ── Konfigurasi path ───────────────────────────────────────────────────────────
HDFS_API_PATH   = os.getenv("HDFS_API_PATH",   "hdfs://namenode:9000/data/airquality/api/")
HDFS_RESULT_PATH = os.getenv("HDFS_RESULT_PATH", "hdfs://namenode:9000/data/airquality/hasil")

# Delta Lake base — di-mount dari host via docker-compose volume
DELTA_BASE  = os.getenv("DELTA_BASE_PATH", "/app/delta_lake")
BRONZE_PATH = f"{DELTA_BASE}/bronze/airquality"
SILVER_PATH = f"{DELTA_BASE}/silver/airquality"
GOLD_PATH   = f"{DELTA_BASE}/gold/airquality_agg"

RESULTS_PATH = Path(
    os.getenv(
        "SPARK_RESULTS_PATH",
        Path(__file__).resolve().parents[1] / "dashboard" / "data" / "spark_results.json",
    )
)
WIB = timezone(timedelta(hours=7))


# ── SparkSession dengan Delta Lake ────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    """
    Buat SparkSession dengan ekstensi Delta Lake.
    configure_spark_with_delta_pip() otomatis menambahkan jar Delta ke classpath.
    Tanpa ini, format "delta" tidak dikenali saat write/read.
    """
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("AQI_Medallion_Jatim")
        .config("spark.driver.memory", "768m")
        .config("spark.driver.bindAddress", "0.0.0.0")
        # Dua config ini wajib agar Delta Lake bisa baca/tulis tabel
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ════════════════════════════════════════════════════════════════════════════════
# LAYER 1 — BRONZE
# Tujuan: simpan data mentah dari HDFS ke Delta Lake dengan metadata tambahan.
# Tidak ada transformasi logis di sini — data harus semirip mungkin aslinya.
# Kenapa penting? Kalau ada bug di Silver/Gold, kita bisa replay dari Bronze
# tanpa harus re-ingest dari HDFS/Kafka.
# ════════════════════════════════════════════════════════════════════════════════
def write_bronze(spark: SparkSession) -> None:
    print("[BRONZE] Membaca JSON dari HDFS...")
    df_raw = (
        spark.read
        .option("multiline", "true")
        .json(HDFS_API_PATH)
    )

    if df_raw.rdd.isEmpty():
        raise RuntimeError("[BRONZE] Tidak ada data di HDFS path: " + HDFS_API_PATH)

    df_bronze = (
        df_raw
        # Tambah kolom metadata — ini TIDAK mengubah data asli,
        # hanya menambah informasi kapan dan dari mana data masuk
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("ispu_api"))
    )

    # Mode "append" → setiap run menambah data, tidak menimpa
    # mergeSchema=true → toleran jika suatu saat ada kolom baru dari API
    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(BRONZE_PATH)
    )
    row_count = df_bronze.count()
    print(f"[BRONZE] Selesai. {row_count} baris ditulis ke {BRONZE_PATH}")


# ════════════════════════════════════════════════════════════════════════════════
# LAYER 2 — SILVER
# Tujuan: bersihkan dan standarisasi data dari Bronze.
# Di sinilah semua "opini" tentang data berada:
#   - Apa yang dianggap duplikat?
#   - Nilai null mana yang dihapus vs diisi?
#   - Bagaimana nama kota distandarisasi?
#   - Kategorisasi AQI menggunakan skala apa?
# ════════════════════════════════════════════════════════════════════════════════
def write_silver(spark: SparkSession) -> None:
    print("[SILVER] Membaca dari Bronze...")
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)

    df_silver = (
        df_bronze
        # --- Cast tipe data ---
        # Di Bronze, semua kolom bisa jadi string karena baca dari JSON mentah.
        # Di Silver, kita pastikan tipe sudah benar untuk komputasi.
        .withColumn("aqi",  col("aqi").cast("double"))
        .withColumn("pm25", col("pm25").cast("double"))
        .withColumn("ts", to_timestamp(col("ingested_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        .withColumn("jam",  hour(col("ts")))

        # --- Standarisasi nama kota ---
        # upper+trim menghilangkan perbedaan seperti "jakarta", "Jakarta ", "JAKARTA"
        # sehingga groupBy("kota") menghasilkan hasil yang konsisten
        .withColumn("kota", upper(trim(col("kota"))))

        # --- Filter data invalid ---
        # Hapus baris yang tidak punya kota atau AQI — tidak bisa dianalisis
        .filter(col("kota").isNotNull() & col("aqi").isNotNull())
        # Filter nilai AQI yang tidak masuk akal secara fisik
        .filter(col("aqi").between(0, 999))

        # --- Deduplikasi ---
        # Jika consumer mengirim data yang sama dua kali (network retry),
        # dropDuplicates memastikan hanya satu yang masuk Silver
        .dropDuplicates(["kota", "ingested_at"])

        # --- Kategorisasi AQI (skala ISPU Indonesia) ---
        # Skala ini sama persis dengan yang dipakai di analysis.py lama
        # Dipindahkan ke Silver agar tersedia untuk semua analisis downstream
        .withColumn(
            "kategori_aqi",
            when(col("aqi") <= 50,  "Baik")
            .when(col("aqi") <= 100, "Sedang")
            .when(col("aqi") <= 200, "Tidak Sehat")
            .otherwise("Berbahaya"),
        )
        .withColumn("_processed_at", current_timestamp())
    )

    # Mode "overwrite" → Silver selalu mencerminkan state terbaru dari Bronze
    # Berbeda dengan Bronze yang append — Silver di-rebuild ulang tiap run
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH)
    )
    row_count = df_silver.count()
    print(f"[SILVER] Selesai. {row_count} baris ditulis ke {SILVER_PATH}")


# ════════════════════════════════════════════════════════════════════════════════
# LAYER 3 — GOLD
# Tujuan: agregasi siap pakai untuk dashboard.
# Gold adalah "produk akhir" dari pipeline — data di sini sudah
# dalam bentuk yang langsung bisa ditampilkan di UI tanpa transformasi lagi.
# ════════════════════════════════════════════════════════════════════════════════
def write_gold(spark: SparkSession) -> None:
    print("[GOLD] Membaca dari Silver...")
    df_silver = spark.read.format("delta").load(SILVER_PATH)

    # Gold tabel 1: ranking kota berdasarkan rata-rata AQI
    # Ini menggantikan run_city_ranking_analysis() lama
    df_gold_ranking = (
        df_silver
        .groupBy("kota")
        .agg(
            spark_round(avg("aqi"), 1).alias("avg_aqi"),
            spark_max("aqi").cast("int").alias("max_aqi"),
            spark_min("aqi").cast("int").alias("min_aqi"),
            spark_sum(when(col("aqi") > 100, 1).otherwise(0)).alias("event_tidak_sehat"),
            count("*").alias("total_data"),
        )
        .withColumn(
            "peringkat",
            rank().over(Window.orderBy(desc("avg_aqi")))
        )
        .withColumn("_aggregated_at", current_timestamp())
    )

    # Gold tabel 2: rata-rata AQI per kota per jam (untuk chart jam puncak)
    df_gold_hourly = (
        df_silver
        .filter(col("jam").isNotNull())
        .groupBy("kota", "jam")
        .agg(
            spark_round(avg("aqi"), 1).alias("avg_aqi"),
            count("*").alias("jumlah_data"),
        )
        .withColumn("_aggregated_at", current_timestamp())
    )

    # Gold tabel 3: distribusi kategori AQI per kota
    df_gold_distribusi = (
        df_silver
        .groupBy("kota", "kategori_aqi")
        .count()
        .withColumnRenamed("count", "jumlah")
    )

    # Simpan semua tabel Gold
    for df, subpath in [
        (df_gold_ranking,   f"{GOLD_PATH}/ranking"),
        (df_gold_hourly,    f"{GOLD_PATH}/hourly"),
        (df_gold_distribusi, f"{GOLD_PATH}/distribusi"),
    ]:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(subpath)
        )
    print(f"[GOLD] Selesai. 3 tabel ditulis ke {GOLD_PATH}")


# ── Fungsi-fungsi analisis (membaca dari Gold, bukan HDFS) ────────────────────
# Fungsi-fungsi di bawah ini adalah versi baru dari fungsi lama di analysis.py.
# Perbedaan utama: sumber data sekarang Gold layer, bukan df_api langsung.
# Logika analisis sendiri tidak berubah.

def run_category_analysis_from_gold(spark: SparkSession):
    """Distribusi kategori AQI per kota — baca dari Gold distribusi."""
    df = spark.read.format("delta").load(f"{GOLD_PATH}/distribusi")

    # Hitung total per kota untuk menghitung persentase
    total_per_kota = df.groupBy("kota").agg(spark_sum("jumlah").alias("total"))
    hasil = (
        df.join(total_per_kota, "kota")
        .withColumn("persentase", spark_round((col("jumlah") / col("total")) * 100, 1))
        .select("kota", col("kategori_aqi").alias("kategori"), "persentase")
        .orderBy("kota", "kategori")
    )
    hasil.write.mode("overwrite").json(f"{HDFS_RESULT_PATH}/analisis1")
    return hasil


def run_hourly_analysis_from_gold(spark: SparkSession):
    """Jam puncak polusi per kota — baca dari Gold hourly."""
    df_hourly = spark.read.format("delta").load(f"{GOLD_PATH}/hourly")

    # Jam puncak = jam dengan avg_aqi tertinggi per kota
    window_kota = Window.partitionBy("kota").orderBy(desc("avg_aqi"))
    jam_puncak = (
        df_hourly
        .withColumn("_rank", rank().over(window_kota))
        .filter(col("_rank") == 1)
        .drop("_rank")
        .withColumnRenamed("avg_aqi", "avg_aqi_puncak")
        .withColumnRenamed("jumlah_data", "jumlah_data_puncak")
        .withColumn(
            "jam_puncak",
            col("jam")
        )
        .withColumn(
            "sesi",
            when((col("jam") >= 7)  & (col("jam") <= 9),  "Pagi Sibuk (07-09)")
            .when((col("jam") >= 17) & (col("jam") <= 19), "Sore Sibuk (17-19)")
            .when((col("jam") >= 10) & (col("jam") <= 16), "Siang (10-16)")
            .when((col("jam") >= 20) & (col("jam") <= 23), "Malam (20-23)")
            .otherwise("Dini Hari (00-06)"),
        )
    )

    df_hourly.write.mode("overwrite").json(f"{HDFS_RESULT_PATH}/analisis2/avg_aqi_per_jam")
    jam_puncak.write.mode("overwrite").json(f"{HDFS_RESULT_PATH}/analisis2/jam_puncak_per_kota")
    return df_hourly, jam_puncak


def run_ranking_from_gold(spark: SparkSession):
    """Ranking kota — baca langsung dari Gold ranking."""
    df_ranking = spark.read.format("delta").load(f"{GOLD_PATH}/ranking")
    df_ranking.write.mode("overwrite").json(f"{HDFS_RESULT_PATH}/analisis3")
    return df_ranking


def add_mllib_predictions(spark: SparkSession, df_silver, results: dict) -> dict:
    """
    Prediksi AQI 24 jam dengan LinearRegression MLlib.
    Fungsi ini TIDAK berubah dari versi lama — hanya sumber data (df_silver)
    yang sekarang lebih bersih karena sudah melalui layer Silver.
    """
    df_ml = (
        df_silver
        .filter(col("aqi").isNotNull() & col("jam").isNotNull() & col("kota").isNotNull())
        .select("kota", "jam", "aqi")
    )

    n_rows = df_ml.count()
    n_cities = df_ml.select("kota").distinct().count()
    if n_rows < 4 or n_cities < 1:
        print(f"[MLlib] Skip: data belum cukup. rows={n_rows}, cities={n_cities}")
        results["prediksi_aqi"] = []
        results["model_metrics"] = {
            "algorithm": "LinearRegression",
            "status": "skipped",
            "reason": "Data training belum cukup",
            "train_rows": n_rows,
            "test_rows": 0,
        }
        return results

    indexer  = StringIndexer(inputCol="kota", outputCol="kota_idx", handleInvalid="keep")
    encoder  = OneHotEncoder(inputCols=["kota_idx"], outputCols=["kota_vec"])
    assembler = VectorAssembler(inputCols=["jam", "kota_vec"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="aqi", regParam=0.1)
    pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])

    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    train_rows = train_df.count()
    test_rows  = test_df.count()
    if train_rows == 0:
        print("[MLlib] Skip: train split kosong")
        return results

    model = pipeline.fit(train_df)
    rmse = r2 = None
    if test_rows > 0:
        preds_test = model.transform(test_df)
        evaluator = RegressionEvaluator(labelCol="aqi", predictionCol="prediction")
        rmse = evaluator.setMetricName("rmse").evaluate(preds_test)
        r2   = evaluator.setMetricName("r2").evaluate(preds_test)

    kota_list = [r["kota"] for r in df_ml.select("kota").distinct().collect()]
    grid_rows = [Row(kota=k, jam=h, aqi=0.0) for k in kota_list for h in range(24)]
    grid_df   = spark.createDataFrame(grid_rows)
    preds_full = (
        model.transform(grid_df)
        .select("kota", "jam", spark_round("prediction", 1).alias("predicted_aqi"))
        .orderBy("kota", "jam")
    )

    results["prediksi_aqi"]  = preds_full.toPandas().to_dict("records")
    results["model_metrics"] = {
        "algorithm": "LinearRegression",
        "features":  ["jam", "kota (one-hot)"],
        "train_rows": train_rows,
        "test_rows":  test_rows,
        "rmse": round(float(rmse), 2) if rmse else None,
        "r2":   round(float(r2), 3)   if r2   else None,
    }
    return results


# ── Export ke dashboard ────────────────────────────────────────────────────────
def export_to_dashboard(spark: SparkSession,
                        hasil_distribusi,
                        hasil_hourly,
                        jam_puncak,
                        ranking,
                        results: dict) -> None:
    """
    Kumpulkan semua hasil analisis dan tulis ke spark_results.json.
    Format JSON tidak berubah → app.py dan index.html tidak perlu dimodifikasi.
    """
    results.update({
        "distribusi_kategori": (
            hasil_distribusi
            .toPandas()
            .to_dict("records")
        ),
        "aqi_per_jam": hasil_hourly.toPandas().to_dict("records"),
        "jam_puncak_per_kota": jam_puncak.toPandas().to_dict("records"),
        "ranking_kota": ranking.toPandas().to_dict("records"),
        "generated_at": datetime.now(WIB).isoformat(),
    })

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_PATH.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"[EXPORT] spark_results.json → {RESULTS_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ── Medallion pipeline ──────────────────────────────────────────────
        print("=" * 60)
        print("Tahap 1/3 — BRONZE: ingest raw data ke Delta Lake")
        write_bronze(spark)

        print("Tahap 2/3 — SILVER: cleaning & standardisasi")
        write_silver(spark)

        print("Tahap 3/3 — GOLD: agregasi untuk dashboard")
        write_gold(spark)
        print("=" * 60)

        # ── Analisis dari Gold ──────────────────────────────────────────────
        print("Menjalankan analisis dari Gold layer...")
        hasil_distribusi          = run_category_analysis_from_gold(spark)
        hasil_hourly, jam_puncak  = run_hourly_analysis_from_gold(spark)
        ranking                   = run_ranking_from_gold(spark)

        # ── MLlib dari Silver (butuh data granular, bukan agregat) ──────────
        df_silver = spark.read.format("delta").load(SILVER_PATH)
        results   = {}
        results   = add_mllib_predictions(spark, df_silver, results)

        # ── Export ke dashboard ─────────────────────────────────────────────
        export_to_dashboard(
            spark,
            hasil_distribusi,
            hasil_hourly,
            jam_puncak,
            ranking,
            results,
        )
        print("[DONE] Pipeline Medallion selesai.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()