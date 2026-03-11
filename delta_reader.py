"""
Delta Lake Medallion Reader

Lit et affiche les données de chaque couche du Delta Lake :
  Bronze : Données brutes CDC (append-only)
  Silver : Activités nettoyées + Employés de référence
  Gold   : Éligibilité Prime sportive & Journées bien-être

Usage :
  docker exec spark-master /opt/spark/bin/spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    /opt/spark/work/delta_reader.py
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

BRONZE_PATH = "/opt/spark/delta/bronze/activities"
SILVER_ACTIVITIES_PATH = "/opt/spark/delta/silver/activities"
SILVER_EMPLOYEES_PATH = "/opt/spark/delta/silver/employees"
GOLD_ELIGIBILITY_PATH = "/opt/spark/delta/gold/employee_eligibility"


def read_layer(spark, path, name):
    """Lit et affiche une couche Delta Lake"""
    print(f"\n{'=' * 60}")
    print(f"  {name}")
    print(f"{'=' * 60}")

    try:
        df = spark.read.format("delta").load(path)
        print(f"Nombre d'enregistrements : {df.count()}")
        print("Schéma :")
        df.printSchema()
        df.show(20, truncate=False)

        # Historique des versions Delta
        delta_table = DeltaTable.forPath(spark, path)
        print(f"--- Historique Delta Lake ({name}) ---")
        delta_table.history().select("version", "timestamp", "operation", "operationMetrics") \
            .show(truncate=False)

    except Exception as e:
        if "is not a Delta table" in str(e) or "doesn't exist" in str(e):
            print(f"  Aucune donnée trouvée pour {name}.")
        else:
            print(f"  Erreur: {e}")


def main():
    spark = SparkSession.builder \
        .appName("Delta Lake Medallion Reader") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        read_layer(spark, BRONZE_PATH, "BRONZE - Activités brutes CDC")
        read_layer(spark, SILVER_ACTIVITIES_PATH, "SILVER - Activités nettoyées")
        read_layer(spark, SILVER_EMPLOYEES_PATH, "SILVER - Employés (référence)")
        read_layer(spark, GOLD_ELIGIBILITY_PATH, "GOLD - Éligibilité employés")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
