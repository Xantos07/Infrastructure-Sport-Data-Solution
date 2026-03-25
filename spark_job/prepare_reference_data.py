"""
Prépare les données de référence (CSV) pour Apache Spark.

Lit les CSV depuis le volume monté (:ro) et les uploade
directement dans MinIO — accessible par le driver ET le worker Spark.
"""

import os
import boto3
from config.configuration import SparkSettings
# a corriger avec les settings de spark dans configuration.py

spark_settings = SparkSettings()

# Lecture — volume monté en :ro depuis ./inputs/ sur le host

CSV_EMPLOYEES = spark_settings.CSV_EMPLOYEES
CSV_SPORT     = spark_settings.CSV_SPORT

# Destination MinIO — s3a://delta-lake/inputs/
MINIO_ENDPOINT = spark_settings.minio_base_url
MINIO_USER     = spark_settings.minio_user
MINIO_PASSWORD = spark_settings.minio_password
BUCKET         = spark_settings.minio_bucket



def upload_csv_file_to_minio(file_path, key):
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
    )
    with open(file_path, "rb") as f:
        s3.put_object(Bucket=BUCKET, Key=key, Body=f)
    print(f"  → Uploadé : s3a://{BUCKET}/{key}")


def main():
    upload_csv_file_to_minio(CSV_EMPLOYEES, "inputs/employees.csv")
    upload_csv_file_to_minio(CSV_SPORT, "inputs/sports.csv")

    print(f"Employés : fichier uploadé depuis {os.path.basename(CSV_EMPLOYEES)}")
    print(f"Sports   : fichier uploadé depuis {os.path.basename(CSV_SPORT)}")


if __name__ == "__main__":
    main()