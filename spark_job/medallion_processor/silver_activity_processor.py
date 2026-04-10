from pyspark.sql import SparkSession, Window
from config.configuration import SparkSettings
from medallion_processor.base_silver_processor import BaseSilverProcessor
from pyspark.sql.functions import (
    col, row_number, desc, current_timestamp, when, lit
)
from delta.tables import DeltaTable

class SilverActivityProcessor(BaseSilverProcessor):
    def __init__(self, spark_session, settings):
        super().__init__(spark_session, settings)

    def reader(self):
        return self.spark.read.format("delta").load(self.settings.delta_bronze_path)

    def quality_checks(self, df):
        # On combine le comptage et la vérification
        bronze_count = df.count()
        if bronze_count == 0:
            print("  Aucune donnée dans la couche Bronze.")
            return False
        
        print(f"  Nombre d'enregistrements CDC dans Bronze : {bronze_count}")

        null_ids = df.filter(col("id").isNull()).count()
        null_employees = df.filter(col("employee_id").isNull()).count()
        if null_ids > 0:
            print(f"  ⚠ QUALITÉ : {null_ids} enregistrement(s) Bronze avec id NULL")
        if null_employees > 0:
            print(f"  ⚠ QUALITÉ : {null_employees} enregistrement(s) Bronze avec employee_id NULL")

        return True
    
    def transform(self, df):
        # 1. Déduplication (Garder la dernière version)
        window = Window.partitionBy("id").orderBy(desc("ingested_at"))
        latest_df = df \
            .withColumn("rn", row_number().over(window)) \
            .filter("rn = 1") \
            .drop("rn")
        
        # 2. Ajout des règles métier (is_commute)
        silver_df = latest_df.select(
            col("id"),
            col("employee_id"),
            col("start_datetime"),
            col("sport_type"),
            col("distance"),
            col("elapsed_time"),
            col("details"),
            col("__deleted"),
            when(col("sport_type").startswith("Déplacement au travail"), lit(True))
                .otherwise(lit(False)).alias("is_commute"),
        )
        return silver_df
    
    def write_silver(self, df):
        # Cette méthode remplace le simple 'overwrite' de la classe mère.
        print(f"Écriture/Merge des données dans la couche Silver : {self.settings.delta_silver_activities}")

        if DeltaTable.isDeltaTable(self.spark, self.settings.delta_silver_activities):
            silver_table = DeltaTable.forPath(self.spark, self.settings.delta_silver_activities)

            silver_table.alias("silver").merge(
                df.alias("new"),
                "silver.id = new.id"
            ).whenMatchedDelete(
                condition="new.__deleted = 'true'"
            ).whenMatchedUpdate(
                condition="""
                    (new.__deleted IS NULL OR new.__deleted != 'true') AND (
                        silver.employee_id    != new.employee_id    OR
                        silver.start_datetime != new.start_datetime OR
                        silver.sport_type     != new.sport_type     OR
                        silver.distance       != new.distance       OR
                        silver.elapsed_time   != new.elapsed_time   OR
                        silver.details        != new.details        OR
                        silver.is_commute     != new.is_commute
                    )
                """,
                set={
                    "employee_id":    "new.employee_id",
                    "start_datetime": "new.start_datetime",
                    "sport_type":     "new.sport_type",
                    "distance":       "new.distance",
                    "elapsed_time":   "new.elapsed_time",
                    "details":        "new.details",
                    "is_commute":     "new.is_commute",
                    "updated_at":     "current_timestamp()"
                }
            ).whenNotMatchedInsert(
                condition="new.__deleted IS NULL OR new.__deleted != 'true'",
                values={
                    "id":             "new.id",
                    "employee_id":    "new.employee_id",
                    "start_datetime": "new.start_datetime",
                    "sport_type":     "new.sport_type",
                    "distance":       "new.distance",
                    "elapsed_time":   "new.elapsed_time",
                    "details":        "new.details",
                    "is_commute":     "new.is_commute",
                    "updated_at":     "current_timestamp()"
                }
            ).execute()

            result_count = self.spark.read.format("delta").load(self.settings.delta_silver_activities).count()
            print(f"  → Silver activities (MERGE) : {result_count} activités")
        else:
            # Traitement initial si la table n'existe pas encore
            active_data = df.filter(
                col("__deleted").isNull() | (col("__deleted") != "true")
            ).drop("__deleted")

            active_data.write.format("delta").mode("overwrite").save(self.settings.delta_silver_activities)

            result_count = active_data.count()
            print(f"  → Silver activities (initial) : {result_count} activités")
     
    def run(self): 
        self.log_step("SILVER - Préparation des données d'activité")
        
        # Le flux est maintenant parfaitement ordonné
        df_bronze = self.reader()
        
        # Si la vérification retourne False (0 ligne), on arrête le traitement ici
        if not self.quality_checks(df_bronze):
            return 
            
        df_transformed = self.transform(df_bronze)
        self.write_silver(df_transformed)