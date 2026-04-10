from medallion_processor.medallion_layer import MedallionLayer
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, coalesce as spark_coalesce, current_timestamp
)

SPORT_TRANSPORT_MODES   = ["Marche/running", "Vélo/Trottinette/Autres"]
MIN_ACTIVITIES_WELLNESS = 15

class GoldProcessor(MedallionLayer):
    def __init__(self, spark_session, settings):
        super().__init__(spark_session, settings)


    def reader(self):
        activities = self.spark.read.format("delta").load(self.settings.delta_silver_activities)
        employees  = self.spark.read.format("delta").load(self.settings.delta_silver_employees)
        return activities, employees
    
    def transform_activities(self, activities):
        return activities.groupBy("employee_id").agg(
        count("*").alias("total_activities"),
        spark_sum(when(col("is_commute") == True, 1).otherwise(0)).alias("total_commute_activities"),
        spark_sum(when(col("is_commute") == False, 1).otherwise(0)).alias("total_external_activities"),
        spark_sum(spark_coalesce(col("distance"),     lit(0))).alias("total_distance_m"),
        spark_sum(spark_coalesce(col("elapsed_time"), lit(0))).alias("total_elapsed_time_s"))

    def transform(self, employees, activities_stats):
        gold_df = employees.join(activities_stats, on="employee_id", how="left") \
            .fillna(0, subset=[
                "total_activities", "total_commute_activities",
                "total_external_activities", "total_distance_m", "total_elapsed_time_s"
            ])
        return gold_df
        

    def prime_sportive_rule(self, df):
        # Règle 1 : Prime sportive — MONTANT calculé dans Power BI (What-If Parameter)
        gold_df = df.withColumn(
            "is_eligible_prime_sportive",
            when(
                (col("transport_mode").isin(SPORT_TRANSPORT_MODES)) &
                (col("total_commute_activities") > 0),
                lit(True)
            ).otherwise(lit(False))
        )

        # Règle 2 : Journées bien-être — NOMBRE de journées paramétrable dans Power BI
        gold_df = gold_df.withColumn(
            "is_eligible_journees_bien_etre",
            when(
                col("total_external_activities") >= MIN_ACTIVITIES_WELLNESS,
                lit(True)
            ).otherwise(lit(False))
        ).withColumn("computed_at", current_timestamp())

        gold_final = gold_df.select(
            "employee_id",
            "nom",
            "prenom",
            "bu",
            "type_de_contrat",
            "transport_mode",
            "external_sport",
            "salaire_brut",                    # fait brut → Prime = salaire_brut * [taux PBI]
            "total_activities",
            "total_commute_activities",
            "total_external_activities",
            "total_distance_m",
            "total_elapsed_time_s",
            "is_eligible_prime_sportive",      # booléen d'éligibilité
            "is_eligible_journees_bien_etre",  # booléen d'éligibilité
            "computed_at"
        )
        return gold_final
    
    def write_gold(self, gold_final, activities):
        # Écriture Gold Delta Lake (MinIO)
        gold_final.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(self.settings.delta_gold_eligibility)

        gold_final.write.mode("overwrite").parquet(self.settings.powerbi_eligibility)
        activities.write.mode("overwrite").parquet(self.settings.powerbi_activities)
        return gold_final, activities

    def print_stats(self, gold_final):
        gold_count        = gold_final.count()
        eligible_prime    = gold_final.filter(col("is_eligible_prime_sportive") == True).count()
        eligible_wellness = gold_final.filter(col("is_eligible_journees_bien_etre") == True).count()

        print(f"  → {gold_count} employés traités → {self.settings.delta_gold_eligibility}")
        print(f"  ├── Éligibles prime sportive                        : {eligible_prime} / {gold_count}")
        print(f"  ├── Éligibles journées bien-être (≥{MIN_ACTIVITIES_WELLNESS} activités) : {eligible_wellness} / {gold_count}")
        print(f"  └── Parquet Power BI : {self.settings.powerbi_eligibility}")
        print(f"                        {self.settings.powerbi_activities}")
        print(f"  ℹ  Montant prime = salaire_brut × [Taux What-If] — calculé dans Power BI")

    def run(self):
        print("Lecture des données Silver...")
        activities, employees = self.reader()

        print("Transformation des données pour la couche Gold...")
        activities_stats = self.transform_activities(activities)
        gold_final = self.transform(employees, activities_stats)
        gold_final =  self.prime_sportive_rule(gold_final)
        print("Écriture des données Gold...")
        gold_final, activities = self.write_gold(gold_final, activities)

        print("Statistiques d'éligibilité :")
        self.print_stats(gold_final)