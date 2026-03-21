#!/bin/bash
set -e

echo "================================================"
echo " Spark Pipeline — Delta Lake Medallion"
echo "================================================"
echo " Master      : ${SPARK_MASTER}"
echo " Delta path  : ${DELTA_PATH}"
echo " MinIO       : ${MINIO_ENDPOINT}"
echo "================================================"

sleep 10

# Préparer les données de référence (Excel → CSV uploadé dans MinIO)
# Les CSV sont écrits sur s3a:// pour être accessibles par driver ET worker
python3 /opt/spark/app/prepare_reference_data.py

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --packages "io.delta:delta-spark_2.12:3.2.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --conf "spark.driver.memory=512m" \
  --conf "spark.executor.memory=512m" \
  --conf "spark.cores.max=1" \
  --conf "spark.executor.cores=1" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.shuffle.partitions=2" \
  --conf "spark.default.parallelism=2" \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_USER}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_PASSWORD}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  /opt/spark/app/spark_medallion_processor.py \
  --mode "${MEDALLION_MODE:-watch}" \
  --interval "${MEDALLION_INTERVAL:-120}"