"""
Prépare les données de référence (Excel → CSV) pour Apache Spark.

Lit les Excel depuis le volume monté (:ro) et uploade les CSV
directement dans MinIO — accessible par le driver ET le worker Spark.
"""

import io
import os
import boto3
import pandas as pd

# Lecture — volume monté en :ro depuis ./inputs/ sur le host
INPUTS_DIR     = "/opt/spark/app/inputs"
XLSX_EMPLOYEES = os.path.join(INPUTS_DIR, "DonneesRH.xlsx")
XLSX_SPORT     = os.path.join(INPUTS_DIR, "DonneesSportive.xlsx")

# Destination MinIO — s3a://delta-lake/inputs/
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER     = os.getenv("MINIO_USER",     "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "minioadmin")
BUCKET         = "delta-lake"


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