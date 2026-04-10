#!/bin/bash
# common.sh — fonctions et configuration partagées entre entrypoint_bronze.sh et entrypoint_silver_gold.sh
# Usage : source /opt/spark/app/common.sh

set -e

# ── Packages Maven partagés (Delta + S3A) ─────────────────────────────────────
# Bronze ajoute spark-sql-kafka en préfixe ; Silver/Gold utilise uniquement ceux-ci.
SPARK_SHARED_PACKAGES="io.delta:delta-spark_2.12:3.2.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262"

# ── Fonctions utilitaires réseau ───────────────────────────────────────────────

normalize_http_endpoint() {
  local endpoint=$1
  endpoint="${endpoint%/}"
  if [[ "${endpoint}" =~ ^https?:// ]]; then
    echo "${endpoint}"
  else
    echo "http://${endpoint}"
  fi
}

extract_host_port() {
  local endpoint=$1
  endpoint="${endpoint#spark://}"
  endpoint="${endpoint#http://}"
  endpoint="${endpoint#https://}"
  endpoint="${endpoint%/}"
  echo "${endpoint}"
}

wait_for_http_service() {
  local url=$1
  local name=$2
  local retries=30
  echo "Waiting for ${name}..."
  until curl -sf "${url}" > /dev/null 2>&1; do
    retries=$((retries - 1))
    [ $retries -eq 0 ] && echo "Error: ${name} is not ready (${url})" && exit 1
    sleep 2
  done
  echo "${name} is ready!"
}

wait_for_tcp_service() {
  local host=$1
  local port=$2
  local name=$3
  local retries=30
  echo "Waiting for ${name} on ${host}:${port}..."
  until (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; do
    retries=$((retries - 1))
    [ $retries -eq 0 ] && echo "Error: ${name} is not reachable on ${host}:${port}" && exit 1
    sleep 2
  done
  echo "${name} is reachable on ${host}:${port}!"
}

# ── Attente des services (MinIO + Spark Master) ────────────────────────────────
# Appelé après avoir défini MINIO_ENDPOINT et SPARK_MASTER dans l'environnement.
wait_for_services() {
  MINIO_BASE_URL=$(normalize_http_endpoint "${MINIO_ENDPOINT}")
  SPARK_MASTER_ADDR=$(extract_host_port "${SPARK_MASTER}")
  SPARK_MASTER_HOST=${SPARK_MASTER_ADDR%%:*}
  SPARK_MASTER_PORT=${SPARK_MASTER_ADDR##*:}

  wait_for_http_service "${MINIO_BASE_URL}/minio/health/live" "MinIO"
  wait_for_tcp_service "${SPARK_MASTER_HOST}" "${SPARK_MASTER_PORT}" "Spark Master RPC"
  wait_for_http_service "http://${SPARK_MASTER_HOST}:8080" "Spark Master UI"

  export MINIO_BASE_URL
  export PYTHONPATH="/opt/spark/app:${PYTHONPATH}"
}

# ── Confs Spark communes ───────────────────────────────────────────────────────
# Utilisé via : spark_common_confs <ui_port>
spark_common_confs() {
  local ui_port=$1
  echo \
    --conf "spark.driver.memory=512m" \
    --conf "spark.executor.memory=512m" \
    --conf "spark.cores.max=1" \
    --conf "spark.executor.cores=1" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.sql.shuffle.partitions=2" \
    --conf "spark.default.parallelism=2" \
    --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_BASE_URL}" \
    --conf "spark.hadoop.fs.s3a.access.key=${MINIO_USER}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_PASSWORD}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.ui.port=${ui_port}" \
    --conf "spark.ui.prometheus.enabled=true"
}
