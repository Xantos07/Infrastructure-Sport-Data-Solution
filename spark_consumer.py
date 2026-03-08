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

# Chemin de stockage Delta Lake
DELTA_PATH = "/opt/spark/delta/activities"
CHECKPOINT_PATH = "/opt/spark/checkpoints/activities_delta"


def consume_activities():
    """Consomme les activités sportives depuis Redpanda et les persiste dans Delta Lake"""
    print("Démarrage du consumer Spark avec Delta Lake...")
    print("-" * 50)

    # Création de la session Spark avec Delta Lake
    spark = SparkSession.builder \
        .appName("Redpanda Consumer - Delta Lake") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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

        # Transformation : convertir le timestamp Debezium (microsecondes) en timestamp lisible
        activities_transformed = activities_df \
            .withColumn("start_datetime", from_unixtime(col("start_timestamp") / 1000000)) \
            .withColumn("ingested_at", current_timestamp())

        # Écriture dans Delta Lake (persistance des données)
        query_delta = activities_transformed.writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .option("path", DELTA_PATH) \
            .queryName("Activities vers Delta Lake") \
            .start()

        # Affichage console en parallèle pour le monitoring
        query_console = activities_transformed.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 50) \
            .option("checkpointLocation", "/opt/spark/checkpoints/activities_console") \
            .queryName("Activities Console Monitor") \
            .start()

        print(f"Données persistées dans Delta Lake : {DELTA_PATH}")
        print("En attente de nouvelles données...")

        # Attendre la fin des deux streams
        spark.streams.awaitAnyTermination()

    except Exception as e:
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    consume_activities()

