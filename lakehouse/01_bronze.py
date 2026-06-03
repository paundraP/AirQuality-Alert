"""
01_bronze.py — Bronze Layer
============================
Tugas ini: baca JSON mentah dari HDFS, tambah metadata, simpan ke Delta Lake.

Kenapa Bronze layer penting?
- Di ETS, Spark langsung baca dari HDFS tanpa ada "checkpoint" data mentah.
  Kalau HDFS korup atau data berubah, tidak ada cara untuk replay.
- Bronze = snapshot data apa adanya dari sumber, tidak ada transformasi logis.
  Kalau nanti Silver/Gold salah, kita tinggal replay dari Bronze.

Cara menjalankan (dari dalam container scheduler atau spark):
  python3 lakehouse/01_bronze.py

Atau langsung dari host:
  docker exec scheduler python3 /app/lakehouse/01_bronze.py
"""

import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# ── Path konfigurasi ──────────────────────────────────────────────────────────
# Sumber: HDFS yang sudah diisi oleh consumer_to_hdfs.py dari ETS
HDFS_API_PATH = os.getenv("HDFS_API_PATH", "hdfs://namenode:9000/data/airquality/api/")
HDFS_RSS_PATH = os.getenv("HDFS_RSS_PATH", "hdfs://namenode:9000/data/airquality/rss/")

# Tujuan: folder lakehouse_data/ di dalam container (persisten via volume mount)
BRONZE_API_PATH = os.getenv("DELTA_BASE_PATH", "/app/delta_lake") + "/bronze/airquality_api"
BRONZE_RSS_PATH = os.getenv("DELTA_BASE_PATH", "/app/delta_lake") + "/bronze/airquality_rss"


# ── SparkSession dengan Delta Lake ────────────────────────────────────────────
def create_spark():
    """
    configure_spark_with_delta_pip() otomatis menambahkan jar delta-spark
    ke classpath PySpark. Dua config 'sql.extensions' dan 'sql.catalog'
    wajib ada agar format "delta" dikenali saat write/read.
    """
    builder = (
        SparkSession.builder
        .appName("Bronze-AirQuality")
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


# ── Ingest satu sumber ke Bronze ──────────────────────────────────────────────
def ingest_to_bronze(spark, hdfs_path: str, delta_path: str, source_name: str):
    """
    Baca JSON dari HDFS, tambah 2 kolom metadata, simpan ke Delta.

    Parameter:
        hdfs_path   : path folder JSON di HDFS
        delta_path  : path tujuan Delta Lake
        source_name : label sumber ("api" atau "rss"), masuk kolom _source
    """
    print(f"\n[BRONZE] Membaca {source_name} dari HDFS: {hdfs_path}")

    df_raw = (
        spark.read
        .option("multiline", "true")   # JSON bisa multi-baris
        .json(hdfs_path)
    )

    row_count = df_raw.count()
    if row_count == 0:
        print(f"[BRONZE] SKIP: tidak ada data di {hdfs_path}")
        return

    print(f"[BRONZE] Ditemukan {row_count} baris. Schema:")
    df_raw.printSchema()

    # Tambah metadata — TIDAK mengubah data asli sama sekali
    # _ingested_at : kapan script ini dijalankan (bukan kapan data dibuat)
    # _source      : dari mana data berasal, berguna untuk filter di Silver/Gold
    df_bronze = (
        df_raw
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit(source_name))
    )

    # mode("append") → tiap run menambah data, tidak menimpa
    # mergeSchema=true → kalau suatu saat API menambah kolom baru, tidak error
    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(delta_path)
    )
    print(f"[BRONZE] Selesai. {row_count} baris → {delta_path}")

    # Tampilkan 3 baris sample untuk verifikasi
    print(f"[BRONZE] Sample data {source_name}:")
    df_bronze.select(
        [c for c in df_raw.columns[:4]] + ["_ingested_at", "_source"]
    ).show(3, truncate=False)


def main():
    spark = create_spark()
    try:
        print("=" * 60)
        print("BRONZE LAYER — AirQuality Alert")
        print("=" * 60)

        # Ingest API (data AQI per kota)
        ingest_to_bronze(spark, HDFS_API_PATH, BRONZE_API_PATH, "api")

        # Ingest RSS (berita lingkungan)
        ingest_to_bronze(spark, HDFS_RSS_PATH, BRONZE_RSS_PATH, "rss")

        print("\n[BRONZE] Pipeline selesai.")
        print(f"  API  → {BRONZE_API_PATH}")
        print(f"  RSS  → {BRONZE_RSS_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
