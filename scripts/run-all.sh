#!/bin/bash
# Usage: bash scripts/run-all.sh
#
# Orchestrator end-to-end pipeline AirQuality Alert.
# Menjalankan setup -> ingestion -> analisis Spark dalam satu perintah.
# Cocok untuk demo dan setup awal A5 (Dashboard).
#
# Setelah selesai, A5 langsung punya:
#   - dashboard/data/spark_results.json   <- dibaca oleh /api/spark
#   - dashboard/data/api/data_*.json      <- live AQI (dibaca oleh /api/live)
#   - dashboard/data/rss/data_*.json      <- berita (dibaca oleh /api/news)
#
# Prasyarat (harus jalan duluan secara manual karena memerlukan sudo / waktu boot):
#   docker compose -f docker-compose-hadoop.yml up -d
#   docker compose -f docker-compose-kafka.yml up -d
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_DIR="$ROOT_DIR/scripts"

SKIP_INIT="${SKIP_INIT:-0}"
SKIP_INGESTION="${SKIP_INGESTION:-0}"
SKIP_SPARK="${SKIP_SPARK:-0}"

print_step() {
  echo ""
  echo "##########################################"
  echo "# $1"
  echo "##########################################"
}

print_step "Pra-check: container Docker"
for c in zookeeper kafka-broker namenode datanode consumer; do
  if ! docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q "true"; then
    echo "[ERROR] Container '$c' belum jalan."
    echo "        Jalankan dulu:"
    echo "          docker compose -f docker-compose-hadoop.yml up -d"
    echo "          docker compose -f docker-compose-kafka.yml up -d"
    exit 1
  fi
done
echo "[OK] Semua container utama aktif."

if [[ "$SKIP_INIT" != "1" ]]; then
  print_step "STEP 1/3: Inisiasi Kafka topics + direktori HDFS"
  bash "$SCRIPT_DIR/init-kafka-topics.sh"
else
  echo "[SKIP] STEP 1 dilewati (SKIP_INIT=1)"
fi

if [[ "$SKIP_INGESTION" != "1" ]]; then
  print_step "STEP 2/3: Jalankan producer API + RSS, tunggu data masuk HDFS"
  bash "$SCRIPT_DIR/run-producers-and-wait-hdfs.sh"
else
  echo "[SKIP] STEP 2 dilewati (SKIP_INGESTION=1)"
fi

if [[ "$SKIP_SPARK" != "1" ]]; then
  print_step "STEP 3/3: Eksekusi Spark notebook -> spark_results.json"
  bash "$SCRIPT_DIR/run-spark-analysis.sh"
else
  echo "[SKIP] STEP 3 dilewati (SKIP_SPARK=1)"
fi

print_step "PIPELINE END-TO-END SELESAI"
echo ""
echo "Output yang siap dipakai A5 (Dashboard):"
echo "  - $ROOT_DIR/dashboard/data/spark_results.json   (untuk /api/spark)"
echo "  - $ROOT_DIR/dashboard/data/api/                 (untuk /api/live)"
echo "  - $ROOT_DIR/dashboard/data/rss/                 (untuk /api/news)"
echo ""
echo "Sekarang A5 tinggal jalanin:"
echo "  cd $ROOT_DIR/dashboard && python3 app.py"
echo ""
echo "Lalu buka http://localhost:5000"
