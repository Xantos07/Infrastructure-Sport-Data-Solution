from pyspark.sql import SparkSession
from config.configuration import SparkSettings
from medallion_processor.base_silver_processor import BaseSilverProcessor
from services.services import normalize_column_name
from pyspark.sql.functions import (
    col, row_number, desc, current_timestamp, when, lit, count,
    sum as spark_sum, coalesce as spark_coalesce
)

# il faut une partie commune de silver puis :
# - une partie employees
# - une partie activities

class SilverEmployeesProcessor(BaseSilverProcessor):
    def __init__(self, spark_session, settings):
        super().__init__(spark_session, settings)

    # mettre dans bronze pour creer un delta ABSOLUMENT !
    def reader_csv(self):
        df_employees = self.spark.read.csv(self.settings.delta_input_employees, header=True, inferSchema=True, encoding="UTF-8")
        df_sports    = self.spark.read.csv(self.settings.delta_input_sports,    header=True, inferSchema=True, encoding="UTF-8")
        df_ref = df_employees.join(df_sports, on="ID salarié", how="left")
        return df_ref
    
    def cleanse(self, df):
        return super().cleanse(df)
    
    def rename_columns(self, df):
        silver_employees = df \
        .withColumnRenamed("id_salarie",          "employee_id") \
        .withColumnRenamed("moyen_de_deplacement", "transport_mode") \
        .withColumnRenamed("pratique_d_un_sport",  "external_sport") \
        .withColumn("updated_at", current_timestamp())
        return silver_employees

    def null_employee_id_count(self, df):
        return df.filter(col("employee_id").isNull()).count()

    def check_data_quality(self, df):
        null_count = self.null_employee_id_count(df)
        # Isoler la logique de qualité des données
        null_count = df.filter(col("employee_id").isNull()).count()
        if null_count > 0:
            print(f"⚠️  Attention : {null_count} enregistrements ont un employee_id null.")
            df.filter(col("employee_id").isNull()).show(10, truncate=False)

    def writing(self, df, path):
        super().writing_silver(df, path)
        
    def run(self):
        self.log_step("SILVER - Préparation des données de référence")
        df_ref = self.reader_csv() # passé a bronze pour creer un delta
        df_clean = self.cleanse(df_ref)
        df_silver = self.rename_columns(df_clean)

        df_silver.cache()
        self.check_data_quality(df_silver)
        self.writing(df_silver, self.settings.delta_silver_employees)
        df_silver.unpersist()

if __name__ == "__main__":
    settings = SparkSettings()
    spark = SparkSession.builder \
        .appName("Silver employees - Bronze to Silver") \
        .getOrCreate()

    processor = SilverEmployeesProcessor(spark, settings)
    processor.run()
