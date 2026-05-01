#!/bin/bash
# Usage:  bash scripts/init-kafka-topics.sh
set -e

BROKER="localhost:9092"
TOPICS=("airquality-api" "airquality-rss")
HDFS_DIRS=("/data/airquality/api" "/data/airquality/rss" "/data/airquality/hasil")

echo "=========================================="
echo " [1/2] Membuat Kafka Topics..."
echo "=========================================="
for TOPIC in "${TOPICS[@]}"; do
  echo "--> Membuat topic: $TOPIC"
  docker exec kafka-broker kafka-topics \
  --bootstrap-server $BROKER \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1
done

echo ""
echo "Daftar topic:"
docker exec kafka-broker kafka-topics --bootstrap-server $BROKER --list

echo ""
echo "=========================================="
echo " [2/2] Membuat Direktori HDFS..."
echo "=========================================="
for DIR in "${HDFS_DIRS[@]}"; do
  echo "--> Membuat: $DIR"
  docker exec namenode hdfs dfs -mkdir -p "$DIR"
  docker exec namenode hdfs dfs -chmod 777 "$DIR"
done

echo ""
echo "Isi /data/airquality:"
docker exec namenode hdfs dfs -ls /data/airquality/

echo ""
echo "=========================================="
echo " SELESAI! Semua anggota sudah bisa mulai coding."
echo "=========================================="