from pyspark.sql import SparkSession

class MedallionLayer:
    def __init__(self, spark_session, settings):
        self.spark = spark_session
        self.settings = settings

    def read_delta(self, path):
        return self.spark.read.format("delta").load(path)

    def write_delta(self, df, path, mode="overwrite"):
        df.write.format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .save(path)
            
    def log_step(self, message):
        print(f"\n{'='*20} {message} {'='*20}")