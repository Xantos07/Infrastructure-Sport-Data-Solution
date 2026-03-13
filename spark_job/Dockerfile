FROM apache/spark:3.5.0

USER root

# ── python→ python3 symlink + pip ───────────────────────────────────────────
RUN ln -s /usr/bin/python3 /usr/bin/python \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /opt/spark/app

# Dependances Python (pandas/openpyxl pour prepare_reference_data.py)
COPY spark_job/requirements.txt ./requirements.txt
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copie du module de configuration partage
COPY config/ ./config/

# Copie des scripts pipeline
COPY spark_job/prepare_reference_data.py .
COPY spark_job/spark_consumer.py .
COPY spark_job/spark_medallion_processor.py .
COPY spark_job/requirements.txt .
COPY spark_job/entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/opt/spark/app/entrypoint.sh"]