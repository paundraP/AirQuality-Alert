import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

from hdfs import InsecureClient
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


ROOT_DIR = Path(__file__).resolve().parents[1]
INTERVAL_SECONDS = int(os.getenv("PIPELINE_INTERVAL_SECONDS", "900"))
CONSUMER_FLUSH_WAIT_SECONDS = int(os.getenv("CONSUMER_FLUSH_WAIT_SECONDS", "75"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-broker:29092")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

TOPICS = ("airquality-api", "airquality-rss")
HDFS_DIRS = (
    "/data/airquality/api",
    "/data/airquality/rss",
    "/data/airquality/hasil",
)


def log(message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def wait_for_kafka():
    while True:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                client_id="airquality-scheduler",
            )
            existing = set(admin.list_topics())
            missing = [topic for topic in TOPICS if topic not in existing]
            if missing:
                try:
                    admin.create_topics(
                        [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in missing],
                        validate_only=False,
                    )
                    log(f"Kafka topics created: {', '.join(missing)}")
                except TopicAlreadyExistsError:
                    pass
            admin.close()
            log("Kafka is ready")
            return
        except Exception as exc:
            log(f"Waiting for Kafka at {KAFKA_BROKER}: {exc}")
            time.sleep(5)


def wait_for_hdfs():
    while True:
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            client.status("/")
            for directory in HDFS_DIRS:
                client.makedirs(directory)
            log("HDFS is ready")
            return client
        except Exception as exc:
            log(f"Waiting for HDFS at {HDFS_URL}: {exc}")
            time.sleep(5)


def hdfs_file_count(client, directory):
    try:
        return len(client.list(directory))
    except Exception:
        return 0


def run_command(args, cwd, extra_env=None):
    log(f"Running: {' '.join(args)}")
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    subprocess.run(args, cwd=cwd, check=True, env=env)


def run_ingestion_cycle(client):
    before_api = hdfs_file_count(client, "/data/airquality/api")
    before_rss = hdfs_file_count(client, "/data/airquality/rss")

    run_command(["python3", "producer_api.py"], ROOT_DIR / "kafka")
    run_command(["python3", "producer_rss.py"], ROOT_DIR / "kafka")

    log(f"Waiting {CONSUMER_FLUSH_WAIT_SECONDS}s for consumer buffer flush")
    time.sleep(CONSUMER_FLUSH_WAIT_SECONDS)

    after_api = hdfs_file_count(client, "/data/airquality/api")
    after_rss = hdfs_file_count(client, "/data/airquality/rss")
    log(f"HDFS files api={after_api} rss={after_rss}")

    if after_api <= before_api and after_rss <= before_rss:
        log("No new HDFS batch detected; Spark will still run on existing data if available")


def run_spark_analysis():
    """
    Jalankan analysis.py (pipeline Medallion).

    Perubahan dari versi lama:
    - Tambahkan PYSPARK_SUBMIT_ARGS agar Delta Lake jar otomatis terpakai
      tanpa perlu spark-submit manual.
    - configure_spark_with_delta_pip() di dalam analysis.py akan men-download
      jar delta-spark sekali saat pertama kali, lalu cache di ~/.ivy2/cache.
    - DELTA_BASE_PATH diteruskan agar analysis.py tahu di mana menyimpan
      Bronze/Silver/Gold (sesuai yang di-mount di docker-compose.yml).
    """
    output_path = ROOT_DIR / "dashboard" / "data" / "spark_results.json"

    # PYSPARK_SUBMIT_ARGS ini memberitahu PySpark untuk menyertakan
    # package delta-spark saat membuat SparkContext.
    # Tanpa ini, `configure_spark_with_delta_pip()` tidak bisa bekerja.
    delta_env = {
        "PYSPARK_SUBMIT_ARGS": (
            "--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell"
        ),
        "DELTA_BASE_PATH": os.getenv("DELTA_BASE_PATH", "/app/delta_lake"),
    }

    run_command(
        ["python3", "analysis.py"],
        ROOT_DIR / "spark",
        extra_env=delta_env,
    )
    log(f"Spark results updated: {output_path}")


def run_forever():
    wait_for_kafka()
    client = wait_for_hdfs()

    while True:
        started = time.time()
        log("Starting scheduled pipeline cycle")
        try:
            run_ingestion_cycle(client)
            run_spark_analysis()
            log("Scheduled pipeline cycle finished")
        except Exception as exc:
            log(f"Scheduled pipeline cycle failed: {exc}")

        elapsed = int(time.time() - started)
        sleep_seconds = max(0, INTERVAL_SECONDS - elapsed)
        log(f"Next cycle in {sleep_seconds}s")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    run_forever()