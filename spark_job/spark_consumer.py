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
from config.configuration import SparkSettings
from schemas import ACTIVITY_DEBEZIUM_SCHEMA

spark_settings = SparkSettings()
# Couche Bronze — stockage MinIO
BRONZE_PATH      = spark_settings.delta_bronze_path
CHECKPOINT_PATH  = spark_settings.checkpoint_bronze
CHECKPOINT_CONSOLE = spark_settings.delta_bronze_console_checkpoint_path
KAFKA_BOOTSTRAP_SERVERS = spark_settings.bootstrap_servers
KAFKA_TOPIC = spark_settings.kafka_topic
KAFKA_STARTING_OFFSETS = spark_settings.kafka_starting_offsets


def consume_activities():
    """Consomme les activités depuis Redpanda et les persiste dans la couche Bronze"""
    print("=" * 60)
    print("BRONZE LAYER - Ingestion streaming Kafka → Delta Lake")
    print("=" * 60)
    print(f"Kafka brokers : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic   : {KAFKA_TOPIC}")


    
    spark = SparkSession.builder \
        .appName("Bronze - Kafka to Delta Lake") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", spark_settings.minio_base_url) \
        .config("spark.hadoop.fs.s3a.access.key", spark_settings.minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", spark_settings.minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    try:
        # Lecture du stream Kafka/Redpanda
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
            .load()

        # Parsing des données JSON depuis Debezium
        activities_df = df.select(
            from_json(col("value").cast("string"), ACTIVITY_DEBEZIUM_SCHEMA).alias("data")
        ).select("data.*")

        # Bronze : transformation minimale + métadonnées d'ingestion
        bronze_df = activities_df \
            .withColumn("start_datetime", from_unixtime(col("start_timestamp") / 1000000)) \
            .withColumn("ingested_at", current_timestamp())

        # Écriture append-only dans Bronze Delta Lake (MinIO)
        query_delta = bronze_df.writeStream \
            .trigger(processingTime="30 seconds") \
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

        query_delta.awaitTermination()

    except Exception as e:
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    consume_activities()