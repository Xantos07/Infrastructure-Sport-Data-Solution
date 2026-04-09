from medallion_processor.medallion_layer import MedallionLayer
from config.configuration import SparkSettings
from pyspark.sql import SparkSession

class GoldProcessor(MedallionLayer):
    def __init__(self, spark_session, settings):
        super().__init__(spark_session, settings)


        
if __name__ == "__main__":
    settings = SparkSettings()
    spark = SparkSession.builder \
        .appName("Gold - Silver to Gold") \
        .getOrCreate()

    processor = GoldProcessor(spark, settings)
    processor.run()
