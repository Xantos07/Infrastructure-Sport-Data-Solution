#!/bin/bash
set -e
 
echo "================================================"
echo " Spark Pipeline — Delta Lake Medallion"
echo "================================================"
echo " Master     : ${SPARK_MASTER}"
echo " Kafka topic: ${KAFKA_TOPIC}"
echo " Delta path : ${DELTA_PATH}"
echo "================================================"
 
sleep 10


exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --packages "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.4.0" \
  --conf "spark.driver.memory=512m" \
  --conf "spark.executor.memory=512m" \
  --conf "spark.cores.max=1" \
  --conf "spark.executor.cores=1" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.shuffle.partitions=2" \
  --conf "spark.default.parallelism=2" \
  /opt/spark/app/spark_consumer.py