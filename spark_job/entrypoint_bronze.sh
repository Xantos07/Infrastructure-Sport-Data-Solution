#!/bin/bash
source /opt/spark/app/common.sh

echo "================================================"
echo " Spark Bronze — Kafka → Delta Lake (MinIO)"
echo "================================================"
echo " Master      : ${SPARK_MASTER}"
echo " Kafka topic : ${KAFKA_TOPIC}"
echo " MinIO       : ${MINIO_ENDPOINT}"
echo "================================================"

wait_for_services

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,${SPARK_SHARED_PACKAGES}" \
  $(spark_common_confs 4040) \
  --conf "spark.sql.streaming.metricsEnabled=true" \
  /opt/spark/app/medallion_processor/bronze_processor.py
