#!/bin/bash
# Usage: bash scripts/run-producers-and-wait-hdfs.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs/ingestion"

API_HDFS_DIR="/data/airquality/api"
RSS_HDFS_DIR="/data/airquality/rss"

POLL_INTERVAL="${POLL_INTERVAL:-5}"
HDFS_TIMEOUT="${HDFS_TIMEOUT:-180}"

API_PID=""
RSS_PID=""

mkdir -p "$LOG_DIR"

cleanup() {
  local exit_code=$?

  if [[ -n "${API_PID}" ]] && kill -0 "${API_PID}" 2>/dev/null; then
    kill "${API_PID}" 2>/dev/null || true
  fi

  if [[ -n "${RSS_PID}" ]] && kill -0 "${RSS_PID}" 2>/dev/null; then
    kill "${RSS_PID}" 2>/dev/null || true
  fi

  exit "${exit_code}"
}

trap cleanup INT TERM EXIT

require_command() {
  local command_name="$1"

  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "[ERROR] Command not found: ${command_name}"
    exit 1
  fi
}

wait_for_running_container() {
  local container_name="$1"
  local timeout_seconds="$2"
  local start_time

  start_time="$(date +%s)"

  echo "[WAIT] Menunggu container ${container_name} aktif..."

  while true; do
    if docker inspect -f '{{.State.Running}}' "${container_name}" 2>/dev/null | grep -q "true"; then
      echo "[OK] Container ${container_name} aktif"
      return 0
    fi

    if (( "$(date +%s)" - start_time >= timeout_seconds )); then
      echo "[ERROR] Timeout menunggu container ${container_name}"
      exit 1
    fi

    sleep "${POLL_INTERVAL}"
  done
}

wait_for_hdfs_ready() {
  local start_time

  start_time="$(date +%s)"

  echo "[WAIT] Menunggu HDFS keluar dari safe mode..."

  while true; do
    if docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then
      echo "[OK] HDFS siap dipakai"
      return 0
    fi

    if (( "$(date +%s)" - start_time >= HDFS_TIMEOUT )); then
      echo "[ERROR] HDFS belum siap setelah ${HDFS_TIMEOUT} detik"
      exit 1
    fi

    sleep "${POLL_INTERVAL}"
  done
}

get_hdfs_file_count() {
  local hdfs_dir="$1"

  docker exec namenode hdfs dfs -count "${hdfs_dir}" 2>/dev/null | awk '{print $2}'
}

wait_for_hdfs_files() {
  local start_time
  local api_count
  local rss_count

  start_time="$(date +%s)"

  echo "[WAIT] Menunggu file masuk ke HDFS..."

  while true; do
    api_count="$(get_hdfs_file_count "${API_HDFS_DIR}" || echo 0)"
    rss_count="$(get_hdfs_file_count "${RSS_HDFS_DIR}" || echo 0)"

    api_count="${api_count:-0}"
    rss_count="${rss_count:-0}"

    echo "[HDFS] api=${api_count} file | rss=${rss_count} file"

    if (( api_count > 0 )) && (( rss_count > 0 )); then
      echo "[OK] File dari kedua producer sudah masuk ke HDFS"
      return 0
    fi

    if (( "$(date +%s)" - start_time >= HDFS_TIMEOUT )); then
      echo "[ERROR] File HDFS belum lengkap setelah ${HDFS_TIMEOUT} detik"
      echo "[INFO] Cek log:"
      echo "       - ${LOG_DIR}/producer_api.log"
      echo "       - ${LOG_DIR}/producer_rss.log"
      exit 1
    fi

    sleep "${POLL_INTERVAL}"
  done
}

start_producer() {
  local script_name="$1"
  local log_name="$2"

  (
    cd "$ROOT_DIR/kafka"
    python3 "${script_name}"
  ) > "${LOG_DIR}/${log_name}" 2>&1 &

  echo $!
}

require_command docker
require_command python3

echo "=========================================="
echo " Pre-flight check"
echo "=========================================="
python3 -c "import kafka, requests, feedparser" >/dev/null
wait_for_running_container "kafka-broker" "${HDFS_TIMEOUT}"
wait_for_running_container "namenode" "${HDFS_TIMEOUT}"
wait_for_running_container "consumer" "${HDFS_TIMEOUT}"
wait_for_hdfs_ready

echo ""
echo "=========================================="
echo " Menjalankan producer"
echo "=========================================="
echo "[RUN] producer_api.py"
API_PID="$(start_producer "producer_api.py" "producer_api.log")"

echo "[RUN] producer_rss.py"
RSS_PID="$(start_producer "producer_rss.py" "producer_rss.log")"

wait "${API_PID}"
echo "[OK] producer_api.py selesai"

wait "${RSS_PID}"
echo "[OK] producer_rss.py selesai"

echo ""
echo "=========================================="
echo " Verifikasi HDFS"
echo "=========================================="
wait_for_hdfs_files

echo ""
echo "Isi HDFS API:"
docker exec namenode hdfs dfs -ls "${API_HDFS_DIR}"

echo ""
echo "Isi HDFS RSS:"
docker exec namenode hdfs dfs -ls "${RSS_HDFS_DIR}"

echo ""
echo "Log producer tersimpan di:"
echo " - ${LOG_DIR}/producer_api.log"
echo " - ${LOG_DIR}/producer_rss.log"
echo ""
echo "SELESAI. Member 3 bisa lanjut kerja di Spark."