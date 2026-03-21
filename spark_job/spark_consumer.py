"""
BRONZE LAYER - Ingestion streaming Kafka → Delta Lake (MinIO)

Consomme les événements CDC (Debezium) depuis Redpanda et les persiste
dans la couche Bronze du Delta Lake (append-only, données brutes).

Architecture Medallion :
  Bronze (ce script) → Silver → Gold → Power BI

Stockage : MinIO (S3-compatible) — s3a://delta-lake/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Schéma des activités (format Debezium CDC)
activity_schema = StructType([
    StructField("id", IntegerType()),
    StructField("employee_id", IntegerType()),
    StructField("start_timestamp", LongType()),
    StructField("sport_type", StringType()),
    StructField("distance", IntegerType()),
    StructField("elapsed_time", IntegerType()),
    StructField("details", StringType()),
    StructField("__deleted", StringType())
])

# Couche Bronze — stockage MinIO
BRONZE_PATH      = "s3a://delta-lake/bronze/activities"
CHECKPOINT_PATH  = "s3a://delta-lake/checkpoints/bronze_activities"
CHECKPOINT_CONSOLE = "s3a://delta-lake/checkpoints/bronze_console"


def consume_activities():
    """Consomme les activités depuis Redpanda et les persiste dans la couche Bronze"""
    print("=" * 60)
    print("BRONZE LAYER - Ingestion streaming Kafka → Delta Lake")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("Bronze - Kafka to Delta Lake") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    try:
        # Lecture du stream Kafka/Redpanda
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "redpanda-0:9092") \
            .option("subscribe", "topic_activities.public.activities") \
            .option("startingOffsets", "earliest") \
            .load()

        # Parsing des données JSON depuis Debezium
        activities_df = df.select(
            from_json(col("value").cast("string"), activity_schema).alias("data")
        ).select("data.*")

        # Bronze : transformation minimale + métadonnées d'ingestion
        bronze_df = activities_df \
            .withColumn("start_datetime", from_unixtime(col("start_timestamp") / 1000000)) \
            .withColumn("ingested_at", current_timestamp())

        # Écriture append-only dans Bronze Delta Lake (MinIO)
        query_delta = bronze_df.writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .option("path", BRONZE_PATH) \
            .queryName("Bronze - Activities") \
            .start()

        # Affichage console en parallèle pour le monitoring
        query_console = bronze_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 50) \
            .option("checkpointLocation", CHECKPOINT_CONSOLE) \
            .queryName("Bronze Console Monitor") \
            .start()

        print(f"Bronze layer : {BRONZE_PATH}")
        print("En attente de nouvelles données...")

        spark.streams.awaitAnyTermination()

    except Exception as e:
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    consume_activities()