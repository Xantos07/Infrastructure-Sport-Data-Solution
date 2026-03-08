from pyspark.sql import SparkSession

DELTA_PATH = "/opt/spark/delta/activities"


def read_delta_activities():
    """Lit et affiche les données persistées dans Delta Lake"""
    print("Lecture des données Delta Lake...")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("Delta Lake Reader") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        # Lecture de la table Delta
        df = spark.read.format("delta").load(DELTA_PATH)

        print(f"\nNombre total d'activités persistées : {df.count()}")
        print(f"Schéma des données :")
        df.printSchema()

        print("\n--- Toutes les activités ---")
        df.show(truncate=False)

        # Statistiques par type de sport
        print("\n--- Activités par type de sport ---")
        df.groupBy("sport_type").count().orderBy("count", ascending=False).show(truncate=False)

        # Statistiques par employé
        print("\n--- Activités par employé ---")
        df.groupBy("employee_id").count().orderBy("count", ascending=False).show(truncate=False)

        # Historique des versions Delta Lake
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, DELTA_PATH)
        print("\n--- Historique des versions Delta Lake ---")
        delta_table.history().show(truncate=False)

    except Exception as e:
        if "is not a Delta table" in str(e) or "doesn't exist" in str(e):
            print("Aucune donnée Delta Lake trouvée. Lancez d'abord le spark_consumer.py.")
        else:
            print(f"Erreur: {e}")
            import traceback
            traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    read_delta_activities()
