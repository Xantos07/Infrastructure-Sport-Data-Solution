#!/bin/bash
source /opt/spark/app/common.sh

echo "================================================"
echo " Spark Pipeline — Delta Lake Medallion"
echo "================================================"
echo " Master      : ${SPARK_MASTER}"
echo " Delta path  : ${DELTA_PATH}"
echo " MinIO       : ${MINIO_ENDPOINT}"
echo "================================================"

wait_for_services

python3 /opt/spark/app/prepare_reference_data.py

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --packages "${SPARK_SHARED_PACKAGES}" \
  $(spark_common_confs 4041) \
  /opt/spark/app/spark_medallion_processor.py \
  --mode "${MEDALLION_MODE:-watch}" \
  --interval "${MEDALLION_INTERVAL:-120}" \
  --wait-bronze \
  --bronze-wait-timeout "${BRONZE_WAIT_TIMEOUT:-900}" \
  --bronze-wait-interval "${BRONZE_WAIT_INTERVAL:-5}"
