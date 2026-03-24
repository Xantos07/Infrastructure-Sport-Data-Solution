"""
Prépare les données de référence (Excel → CSV) pour Apache Spark.

Lit les Excel depuis le volume monté (:ro) et uploade les CSV
directement dans MinIO — accessible par le driver ET le worker Spark.
"""

import io
import os
import boto3
import pandas as pd
from config.configuration import SparkSettings
# a corriger avec les settings de spark dans configuration.py

spark_settings = SparkSettings()

# Lecture — volume monté en :ro depuis ./inputs/ sur le host

XLSX_EMPLOYEES = spark_settings.XLSX_EMPLOYEES
XLSX_SPORT     = spark_settings.XLSX_SPORT

# Destination MinIO — s3a://delta-lake/inputs/
MINIO_ENDPOINT = spark_settings.minio_base_url
MINIO_USER     = spark_settings.minio_user
MINIO_PASSWORD = spark_settings.minio_password
BUCKET         = spark_settings.minio_bucket



def upload_csv_to_minio(df, key):
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
    )
    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf)
    print(f"  → Uploadé : s3a://{BUCKET}/{key}")


def main():
    df_rh    = pd.read_excel(XLSX_EMPLOYEES)
    df_sport = pd.read_excel(XLSX_SPORT)

    upload_csv_to_minio(df_rh,    "inputs/employees.csv")
    upload_csv_to_minio(df_sport, "inputs/sports.csv")

    print(f"Employés : {len(df_rh)} lignes | {list(df_rh.columns)}")
    print(f"Sports   : {len(df_sport)} lignes | {list(df_sport.columns)}")


if __name__ == "__main__":
    main()