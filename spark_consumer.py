from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Schéma des activités

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

def consume_activities():
    """Consomme et affiche les activités sportives en temps réel"""
    print("Démarrage du consumer Spark pour les activités...")
    print("-" * 50)
    
    # Création de la session Spark
    spark = SparkSession.builder \
        .appName("Redpanda Consumer") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    try:
        # Lecture du stream Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "redpanda-0:9092") \
            .option("subscribe", "topic_activities.public.activities") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parsing des données JSON
        activities_df = df.select(
            from_json(col("value").cast("string"), activity_schema).alias("data")
        ).select("data.*")
        
        # Affichage en temps réel
        query = activities_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 50) \
            .option("checkpointLocation", "/opt/spark/checkpoints/activities") \
            .queryName("Activités Sportives") \
            .start()
        
        # Maintenir le stream actif
        query.awaitTermination()
    
    except Exception as e:
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    consume_activities()

