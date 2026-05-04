#!/bin/bash
# Usage: bash scripts/run-spark-analysis.sh
#
# Menjalankan spark/analysis.ipynb di dalam container `consumer`
# (yang sudah punya jaringan ke namenode:9000 + Python 3.11 siap pakai),
# lalu menyalin hasil dashboard/data/spark_results.json ke host.
#
# Output untuk A5 (Dashboard):
#   - dashboard/data/spark_results.json   (di host, dibaca oleh Flask)
#   - spark/analysis.executed.ipynb       (notebook ter-eksekusi, untuk lampiran laporan)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs/spark"
mkdir -p "$LOG_DIR" "$ROOT_DIR/dashboard/data"

CONTAINER="consumer"
WORKDIR_IN_CONTAINER="/app/spark_run"
NOTEBOOK_LOCAL="$ROOT_DIR/spark/analysis.ipynb"
NOTEBOOK_EXECUTED_LOCAL="$ROOT_DIR/spark/analysis.executed.ipynb"
RESULTS_LOCAL="$ROOT_DIR/dashboard/data/spark_results.json"

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "[ERROR] Command tidak ditemukan: $cmd"
    exit 1
  fi
}

wait_for_running_container() {
  local container_name="$1"
  local timeout_seconds="${2:-60}"
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
    sleep 3
  done
}

require_command docker

if [[ ! -f "$NOTEBOOK_LOCAL" ]]; then
  echo "[ERROR] Notebook tidak ditemukan: $NOTEBOOK_LOCAL"
  exit 1
fi

echo "=========================================="
echo " [1/5] Verifikasi container & data HDFS"
echo "=========================================="
wait_for_running_container "$CONTAINER" 60
wait_for_running_container "namenode" 60

API_COUNT="$(docker exec namenode hdfs dfs -count /data/airquality/api 2>/dev/null | awk '{print $2}' || echo 0)"
API_COUNT="${API_COUNT:-0}"
if (( API_COUNT == 0 )); then
  echo "[ERROR] /data/airquality/api masih kosong di HDFS."
  echo "        Jalankan dulu: bash scripts/run-producers-and-wait-hdfs.sh"
  exit 1
fi
echo "[OK] /data/airquality/api berisi ${API_COUNT} file"

echo ""
echo "=========================================="
echo " [2/5] Install dependency Spark di container"
echo "=========================================="
# Java 17 dibutuhkan PySpark 3.5+. Image python:3.11-slim belum punya Java.
# Idempoten: install hanya jika belum ada.
docker exec "$CONTAINER" bash -c '
  set -e
  if ! command -v java >/dev/null 2>&1; then
    echo "[INSTALL] OpenJDK 17 + procps..."
    apt-get update -qq
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps >/dev/null
  else
    echo "[SKIP] Java sudah terpasang: $(java -version 2>&1 | head -n1)"
  fi
  if ! python3 -c "import pyspark, pandas, nbclient, nbformat" >/dev/null 2>&1; then
    echo "[INSTALL] pyspark + jupyter tooling..."
    pip install --no-cache-dir --quiet pyspark==3.5.1 pandas==2.2.2 nbclient==0.10.0 nbformat==5.10.4 ipykernel==6.29.5
  else
    echo "[SKIP] pyspark + jupyter sudah terpasang"
  fi
' 2>&1 | tee "$LOG_DIR/install.log"

echo ""
echo "=========================================="
echo " [3/5] Copy notebook ke container"
echo "=========================================="
docker exec "$CONTAINER" mkdir -p "$WORKDIR_IN_CONTAINER/spark" "$WORKDIR_IN_CONTAINER/dashboard/data"
docker cp "$NOTEBOOK_LOCAL" "$CONTAINER:$WORKDIR_IN_CONTAINER/spark/analysis.ipynb"
echo "[OK] Notebook tersalin ke $CONTAINER:$WORKDIR_IN_CONTAINER/spark/analysis.ipynb"

echo ""
echo "=========================================="
echo " [4/5] Eksekusi notebook (PySpark)"
echo "=========================================="
echo "Log akan tampil di terminal & tersimpan di $LOG_DIR/execute.log"
echo ""

# Jalankan via nbclient supaya output cell ikut ter-embed di .ipynb hasil eksekusi.
# JAVA_HOME diset ke OpenJDK 17 yang baru di-install.
docker exec \
  -e JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64 \
  -e PYSPARK_PYTHON=python3 \
  "$CONTAINER" bash -c "
    set -e
    cd $WORKDIR_IN_CONTAINER/spark
    # Auto-detect arsitektur untuk JAVA_HOME (arm64 atau amd64)
    if [[ ! -d \"\$JAVA_HOME\" ]]; then
      export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f \$(which java))))
    fi
    echo \"JAVA_HOME=\$JAVA_HOME\"
    jupyter execute analysis.ipynb \
        --output analysis.executed.ipynb \
        --timeout=600 \
        2>&1 || {
      echo '[INFO] jupyter execute gagal, fallback ke jupyter nbconvert'
      jupyter nbconvert --to notebook --execute analysis.ipynb \
        --output analysis.executed.ipynb \
        --ExecutePreprocessor.timeout=600
    }
  " 2>&1 | tee "$LOG_DIR/execute.log"

echo ""
echo "=========================================="
echo " [5/5] Salin hasil ke host"
echo "=========================================="

# Salin spark_results.json ke dashboard/data/ host
docker cp "$CONTAINER:$WORKDIR_IN_CONTAINER/dashboard/data/spark_results.json" "$RESULTS_LOCAL"
echo "[OK] $RESULTS_LOCAL"

# Salin notebook ter-eksekusi (untuk lampiran laporan / bukti)
docker cp "$CONTAINER:$WORKDIR_IN_CONTAINER/spark/analysis.executed.ipynb" "$NOTEBOOK_EXECUTED_LOCAL"
echo "[OK] $NOTEBOOK_EXECUTED_LOCAL"

echo ""
echo "Ringkasan file:"
ls -la "$RESULTS_LOCAL" "$NOTEBOOK_EXECUTED_LOCAL"
echo ""
echo "Top-level keys di spark_results.json:"
python3 -c "
import json
d = json.load(open('$RESULTS_LOCAL'))
for k, v in d.items():
    if isinstance(v, list):
        print(f'  - {k}: {len(v)} rows')
    elif isinstance(v, dict):
        print(f'  - {k}: dict ({len(v)} keys)')
    else:
        print(f'  - {k}: {v}')
"

echo ""
echo "=========================================="
echo " SELESAI! A5 sudah punya data untuk dashboard."
echo "=========================================="
echo ""
echo "Endpoint Flask /api/spark akan membaca dari:"
echo "  $RESULTS_LOCAL"
