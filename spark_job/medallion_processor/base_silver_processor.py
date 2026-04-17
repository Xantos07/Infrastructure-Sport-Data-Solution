from pyspark.sql import SparkSession
from config.configuration import SparkSettings
from medallion_processor.medallion_layer import MedallionLayer
from services.services import normalize_column_name

class BaseSilverProcessor(MedallionLayer):
    def __init__(self, spark_session, settings):
        super().__init__(spark_session, settings)

    def cleanse(self, df):
        for col in df.columns:
            df = df.withColumnRenamed(col, normalize_column_name(col))
        return df

    def writing_silver(self, df, path):
        df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(path)
        