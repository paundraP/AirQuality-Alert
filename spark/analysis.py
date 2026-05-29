import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    desc,
    first,
    hour,
    rank,
    round as spark_round,
    to_timestamp,
    when,
)
from pyspark.sql.window import Window


HDFS_API_PATH = os.getenv("HDFS_API_PATH", "hdfs://namenode:9000/data/airquality/api/")
HDFS_RESULT_PATH = os.getenv("HDFS_RESULT_PATH", "hdfs://namenode:9000/data/airquality/hasil")
RESULTS_PATH = Path(
    os.getenv(
        "SPARK_RESULTS_PATH",
        Path(__file__).resolve().parents[1] / "dashboard" / "data" / "spark_results.json",
    )
)
WIB = timezone(timedelta(hours=7))


def create_spark_session():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Analisis_AQI_Jatim")
        .config("spark.driver.memory", "512m")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )


def load_api_data(spark):
    df_api_raw = (
        spark.read
        .option("multiline", "true")
        .json(HDFS_API_PATH)
    )

    df_api = (
        df_api_raw
        .withColumn("aqi", col("aqi").cast("double"))
        .withColumn("pm25", col("pm25").cast("double"))
        .withColumn("ts", to_timestamp(col("ingested_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        .withColumn("jam", hour(col("ts")))
        .filter(col("kota").isNotNull() & col("aqi").isNotNull())
    )

    print("Schema data API:")
    df_api.printSchema()
    df_api.select("kota", "aqi", "ingested_at", "ts", "jam").show(5, truncate=False)
    return df_api


def run_category_analysis(df_api):
    df_classified = df_api.withColumn(
        "kategori_baru",
        when(col("aqi") <= 50, "Baik")
        .when((col("aqi") > 50) & (col("aqi") <= 100), "Sedang")
        .when((col("aqi") > 100) & (col("aqi") <= 200), "Tidak Sehat")
        .otherwise("Berbahaya"),
    )

    total_kota = df_classified.groupBy("kota").count().withColumnRenamed("count", "total")
    distribusi = df_classified.groupBy("kota", "kategori_baru").count()
    hasil_analisis = (
        distribusi.join(total_kota, "kota")
        .withColumn("persentase", spark_round((col("count") / col("total")) * 100, 1))
        .select("kota", "kategori_baru", "persentase")
        .orderBy("kota", "kategori_baru")
    )

    hasil_analisis.write.mode("overwrite").json(f"{HDFS_RESULT_PATH}/analisis1")
    print(f"Analisis 1 tersimpan ke HDFS: {HDFS_RESULT_PATH}/analisis1")
    return df_classified, hasil_analisis


def run_hourly_analysis(df_api):
    df_api.createOrReplaceTempView("aqi_ts")
    hasil2 = df_api.sparkSession.sql(
        """
        SELECT kota,
               HOUR(ts)            AS jam,
               ROUND(AVG(aqi), 1)  AS avg_aqi,
               COUNT(*)            AS jumlah_data
        FROM   aqi_ts
        GROUP  BY kota, HOUR(ts)
        ORDER  BY kota, avg_aqi DESC
        """
    )

    jam_puncak = hasil2.groupBy("kota").agg(
        first("jam").alias("jam_puncak"),
        first("avg_aqi").alias("avg_aqi_puncak"),
        first("jumlah_data").alias("jumlah_data_puncak"),
    ).orderBy("kota")

    jam_puncak_labeled = jam_puncak.withColumn(
        "sesi",
        when((col("jam_puncak") >= 7) & (col("jam_puncak") <= 9), "Pagi Sibuk (07-09)")
        .when((col("jam_puncak") >= 17) & (col("jam_puncak") <= 19), "Sore Sibuk (17-19)")
        .when((col("jam_puncak") >= 10) & (col("jam_puncak") <= 16), "Siang (10-16)")
        .when((col("jam_puncak") >= 20) & (col("jam_puncak") <= 23), "Malam (20-23)")
        .otherwise("Dini Hari (00-06)"),
    )

    output_path = f"{HDFS_RESULT_PATH}/analisis2"
    hasil2.write.mode("overwrite").json(f"{output_path}/avg_aqi_per_jam")
    jam_puncak_labeled.write.mode("overwrite").json(f"{output_path}/jam_puncak_per_kota")
    print(f"Analisis 2 tersimpan ke HDFS: {output_path}")
    return hasil2, jam_puncak_labeled


def run_city_ranking_analysis(df_classified):
    df_classified.createOrReplaceTempView("aqi_data")
    hasil3 = df_classified.sparkSession.sql(
        """
        SELECT kota,
               ROUND(AVG(aqi), 1)                         AS avg_aqi,
               MAX(aqi)                                   AS max_aqi,
               MIN(aqi)                                   AS min_aqi,
               SUM(CASE WHEN aqi > 100 THEN 1 ELSE 0 END) AS event_tidak_sehat,
               COUNT(*)                                   AS total_data
        FROM   aqi_data
        GROUP  BY kota
        ORDER  BY avg_aqi DESC
        """
    )

    ranking_kota = (
        hasil3.withColumn("peringkat", rank().over(Window.orderBy(desc("avg_aqi"))))
        .select(
            "peringkat",
            "kota",
            "avg_aqi",
            "max_aqi",
            "min_aqi",
            "event_tidak_sehat",
            "total_data",
        )
        .orderBy("peringkat")
    )

    output_path = f"{HDFS_RESULT_PATH}/analisis3"
    ranking_kota.write.mode("overwrite").json(output_path)
    print(f"Analisis 3 tersimpan ke HDFS: {output_path}")
    return ranking_kota


def write_dashboard_results(hasil_analisis, hasil2, jam_puncak_labeled, ranking_kota):
    distribusi_records = (
        hasil_analisis
        .withColumnRenamed("kategori_baru", "kategori")
        .toPandas()
        .to_dict("records")
    )

    results = {
        "distribusi_kategori": distribusi_records,
        "aqi_per_jam": hasil2.toPandas().to_dict("records"),
        "ranking_kota": ranking_kota.toPandas().to_dict("records"),
        "jam_puncak_per_kota": jam_puncak_labeled.toPandas().to_dict("records"),
        "generated_at": datetime.now(WIB).isoformat(),
    }

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_PATH.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)

    print(f"spark_results.json tersimpan ke: {RESULTS_PATH}")
    return results


def add_mllib_predictions(df_api, results):
    df_ml = (
        df_api
        .filter(col("aqi").isNotNull() & col("jam").isNotNull() & col("kota").isNotNull())
        .select("kota", "jam", "aqi")
    )

    n_rows = df_ml.count()
    n_cities = df_ml.select("kota").distinct().count()
    if n_rows < 4 or n_cities < 1:
        print(f"Skip MLlib: data training belum cukup. rows={n_rows}, cities={n_cities}")
        results["prediksi_aqi"] = []
        results["model_metrics"] = {
            "algorithm": "LinearRegression",
            "status": "skipped",
            "reason": "Data training belum cukup",
            "train_rows": n_rows,
            "test_rows": 0,
        }
        return results

    indexer = StringIndexer(inputCol="kota", outputCol="kota_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["kota_idx"], outputCols=["kota_vec"])
    assembler = VectorAssembler(inputCols=["jam", "kota_vec"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="aqi", regParam=0.1)
    pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])

    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    train_rows = train_df.count()
    test_rows = test_df.count()
    if train_rows == 0:
        print("Skip MLlib: train split kosong")
        return results

    model = pipeline.fit(train_df)
    if test_rows > 0:
        predictions_test = model.transform(test_df)
        rmse = RegressionEvaluator(
            labelCol="aqi",
            predictionCol="prediction",
            metricName="rmse",
        ).evaluate(predictions_test)
        r2 = RegressionEvaluator(
            labelCol="aqi",
            predictionCol="prediction",
            metricName="r2",
        ).evaluate(predictions_test)
    else:
        rmse = None
        r2 = None

    kota_list = [r["kota"] for r in df_ml.select("kota").distinct().collect()]
    grid_rows = [Row(kota=k, jam=h, aqi=0.0) for k in kota_list for h in range(24)]
    grid_df = df_api.sparkSession.createDataFrame(grid_rows)
    preds_full = (
        model.transform(grid_df)
        .select("kota", "jam", spark_round("prediction", 1).alias("predicted_aqi"))
        .orderBy("kota", "jam")
    )

    results["prediksi_aqi"] = preds_full.toPandas().to_dict("records")
    results["model_metrics"] = {
        "algorithm": "LinearRegression",
        "features": ["jam", "kota (one-hot)"],
        "train_rows": train_rows,
        "test_rows": test_rows,
        "rmse": round(float(rmse), 2) if rmse is not None else None,
        "r2": round(float(r2), 3) if r2 is not None else None,
    }
    results["generated_at"] = datetime.now(WIB).isoformat()
    return results


def save_results(results):
    with RESULTS_PATH.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"spark_results.json final tersimpan ke: {RESULTS_PATH}")


def main():
    spark = create_spark_session()
    try:
        df_api = load_api_data(spark)
        if df_api.count() == 0:
            raise RuntimeError("Tidak ada data API valid di HDFS untuk dianalisis")

        df_classified, hasil_analisis = run_category_analysis(df_api)
        hasil2, jam_puncak_labeled = run_hourly_analysis(df_api)
        ranking_kota = run_city_ranking_analysis(df_classified)
        results = write_dashboard_results(
            hasil_analisis,
            hasil2,
            jam_puncak_labeled,
            ranking_kota,
        )
        results = add_mllib_predictions(df_api, results)
        save_results(results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
