#!/bin/bash
set -e

echo "================================================"
echo " Spark Bronze — Kafka → Delta Lake (MinIO)"
echo "================================================"
echo " Master      : ${SPARK_MASTER}"
echo " Kafka topic : ${KAFKA_TOPIC}"
echo " MinIO       : ${MINIO_ENDPOINT}"
echo "================================================"

sleep 10

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
io.delta:delta-spark_2.12:3.2.0,\
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
  /opt/spark/app/spark_consumer.py