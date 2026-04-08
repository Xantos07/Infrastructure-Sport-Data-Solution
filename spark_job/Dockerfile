FROM apache/spark:3.5.0

USER root

# ── python→ python3 symlink + pip ───────────────────────────────────────────
RUN ln -s /usr/bin/python3 /usr/bin/python \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN /opt/spark/bin/spark-submit \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
io.delta:delta-spark_2.12:3.2.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --dry-run /dev/null 2>&1 | grep -E '(ERROR|WARNING|Downloading)' || true
  
WORKDIR /opt/spark/app

# Dependances Python (pandas/openpyxl pour prepare_reference_data.py)
COPY spark_job/requirements.txt ./requirements.txt
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copie du module de configuration partage
COPY config/ ./config/

# Copie des scripts pipeline
COPY spark_job/prepare_reference_data.py .
COPY spark_job/schemas.py .
COPY spark_job/spark_medallion_processor.py .
COPY spark_job/medallion_processor/bronze_processor.py .
COPY spark_job/medallion_processor/silver_processor.py .
COPY spark_job/medallion_processor/gold_processor.py .
COPY spark_job/medallion_layer.py .
COPY spark_job/requirements.txt .
COPY spark_job/entrypoint_silver_gold.sh .
COPY spark_job/entrypoint_bronze.sh .
RUN chmod +x entrypoint_silver_gold.sh entrypoint_bronze.sh

# ENTRYPOINT ["/opt/spark/app/entrypoint_silver_gold.sh"]