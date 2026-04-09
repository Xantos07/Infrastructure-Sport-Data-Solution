from medallion_processor.medallion_layer import MedallionLayer
from schemas import ACTIVITY_DEBEZIUM_SCHEMA
from pyspark.sql.functions import from_json, col, from_unixtime, current_timestamp
from pyspark.sql import SparkSession
from config.configuration import SparkSettings

class BronzeProcessor(MedallionLayer):
    def __init__(self, spark_session, settings, name="Bronze Processor"):
        super().__init__(spark_session, settings)
        self.name = name

    def reader(self):
        """Lecture du stream Kafka/Redpanda et parsing JSON"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.settings.bootstrap_servers) \
            .option("subscribe", self.settings.kafka_topic) \
            .option("startingOffsets", self.settings.kafka_starting_offsets) \
            .load()

    def _transform(self, df):
        activities_df = df.select(
            from_json(col("value").cast("string"), ACTIVITY_DEBEZIUM_SCHEMA).alias("data")
        ).select("data.*")

        bronze_df = activities_df \
            .withColumn("start_datetime", from_unixtime(col("start_timestamp") / 1000000)) \
            .withColumn("ingested_at", current_timestamp())

        return bronze_df

    def writer(self, df):
        """Écriture du DataFrame transformé dans la couche Bronze (Delta Lake)"""
        df.writeStream \
            .trigger(processingTime="30 seconds") \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", self.settings.checkpoint_bronze) \
            .option("path", self.settings.delta_bronze_path) \
            .queryName("Bronze - Activities") \
            .start()

    def console_monitoring(self, df):
        df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 50) \
            .option("checkpointLocation", self.settings.delta_bronze_console_checkpoint_path) \
            .queryName("Bronze Console Monitor") \
            .start()

    def run(self):
        self.log_step("BRONZE - Ingestion et Nettoyage")
        df = self.reader()
        transformed_df = self._transform(df)
        self.writer(transformed_df)
        self.console_monitoring(transformed_df)
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    settings = SparkSettings()
    spark = SparkSession.builder \
        .appName("Bronze - Kafka to Delta Lake") \
        .getOrCreate()

    processor = BronzeProcessor(spark, settings)
    processor.run()
