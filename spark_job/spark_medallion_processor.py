"""
MEDALLION PROCESSOR - Bronze → Silver → Gold

Traite les données du Delta Lake selon l'architecture Medallion :
  Bronze : Données brutes CDC (append-only) depuis Kafka
  Silver : Données nettoyées, dédupliquées (MERGE) + table de référence employés
  Gold   : Métriques métier — Éligibilité Prime sportive & Journées bien-être

Produit également des fichiers Parquet pour Power BI Desktop.

Usage :
  docker exec spark-master /opt/spark/bin/spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    /opt/spark/work/spark_medallion_processor.py --mode single

  Modes :
    --mode single : Traitement unique puis arrêt
    --mode watch  : Traitement continu toutes les N secondes (défaut: 60s)
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, desc, current_timestamp, when, lit, count,
    sum as spark_sum, coalesce as spark_coalesce
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================
# Chemins Delta Lake — Architecture Medallion
# ============================================================
BRONZE_PATH = "/opt/spark/delta/bronze/activities"

SILVER_ACTIVITIES_PATH = "/opt/spark/delta/silver/activities"
SILVER_EMPLOYEES_PATH = "/opt/spark/delta/silver/employees"

GOLD_ELIGIBILITY_PATH = "/opt/spark/delta/gold/employee_eligibility"

# ============================================================
# Sorties Parquet pour Power BI Desktop
# ============================================================
POWERBI_ELIGIBILITY = "/opt/spark/powerbi_data/employee_eligibility.parquet"
POWERBI_ACTIVITIES = "/opt/spark/powerbi_data/activities.parquet"

# ============================================================
# Données de référence (CSV préparés depuis Excel)
# ============================================================
REF_EMPLOYEES_CSV = "/opt/spark/delta/inputs/employees.csv"
REF_SPORTS_CSV    = "/opt/spark/delta/inputs/sports.csv"

# ============================================================
# Règles métier
# ============================================================
# Modes de transport considérés comme sportifs
SPORT_TRANSPORT_MODES = ["Marche/running", "Vélo/Trottinette/Autres"]
# Nombre minimum d'activités externes pour les journées bien-être
MIN_ACTIVITIES_WELLNESS = 15


def create_spark_session():
    """Crée une session Spark avec support Delta Lake"""
    return SparkSession.builder \
        .appName("Medallion Processor - Bronze/Silver/Gold") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()



# ============================================================
# SILVER LAYER
# ============================================================

def process_silver_employees(spark):
    """Silver : charge les données de référence employés (CSV → Delta Lake)"""
    print("\n" + "=" * 60)
    print("SILVER - Données de référence employés")
    print("=" * 60)

    # Lecture des CSV de référence
    df_employees = spark.read.csv(REF_EMPLOYEES_CSV, header=True, inferSchema=True,
                                  encoding="UTF-8")
    df_sports = spark.read.csv(REF_SPORTS_CSV, header=True, inferSchema=True,
                               encoding="UTF-8")

    # Jointure RH + Sports sur l'ID salarié
    df_ref = df_employees.join(df_sports, on="ID salarié", how="left")

    # Normalisation de TOUS les noms de colonnes pour Delta Lake
    # (suppression espaces, accents, apostrophes → underscores)
    def normalize_column_name(name):
        import unicodedata
        # Supprimer les accents
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        # Remplacer espaces, apostrophes par underscore
        name = name.replace(' ', '_').replace("'", '_').replace('é', 'e').replace('è', 'e')
        return name.lower()

    for old_col in df_ref.columns:
        new_col = normalize_column_name(old_col)
        df_ref = df_ref.withColumnRenamed(old_col, new_col)

    # Renommage final des colonnes clés pour cohérence avec le reste du pipeline
    silver_employees = df_ref \
        .withColumnRenamed("id_salarie", "employee_id") \
        .withColumnRenamed("moyen_de_deplacement", "transport_mode") \
        .withColumnRenamed("pratique_d_un_sport", "external_sport") \
        .withColumn("updated_at", current_timestamp())
    
    # Les colonnes normalisées : nom, prenom, bu, type_de_contrat, date_de_naissance, 
    # date_d_embauche, nombre_de_jours_de_cp, adresse_du_domicile, salaire_brut

    # Écriture dans Silver (overwrite : données de référence statiques)
    silver_employees.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_EMPLOYEES_PATH)

    nb = silver_employees.count()
    print(f"  → {nb} employés chargés dans {SILVER_EMPLOYEES_PATH}")
    return nb


def process_silver_activities(spark):
    """Silver : déduplique les événements CDC de Bronze et applique MERGE"""
    print("\n" + "=" * 60)
    print("SILVER - Traitement Bronze → Silver (activités)")
    print("=" * 60)

    # Lecture de la couche Bronze
    try:
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    except Exception as e:
        print(f"  Aucune donnée Bronze trouvée : {e}")
        return 0

    bronze_count = bronze_df.count()
    if bronze_count == 0:
        print("  Aucune donnée dans la couche Bronze.")
        return 0

    print(f"  Bronze : {bronze_count} événements CDC")

    # Déduplication : garder le dernier événement par ID d'activité
    window = Window.partitionBy("id").orderBy(desc("ingested_at"))
    latest_df = bronze_df \
        .withColumn("rn", row_number().over(window)) \
        .filter("rn = 1") \
        .drop("rn")

    # Ajout du flag is_commute (déplacement au travail vs activité externe)
    silver_data = latest_df.select(
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
        current_timestamp().alias("updated_at")
    )

    # Vérifier si la table Silver existe déjà pour faire un MERGE
    try:
        silver_table = DeltaTable.forPath(spark, SILVER_ACTIVITIES_PATH)

        # MERGE : upsert des enregistrements actifs, suppression des supprimés
        silver_table.alias("silver").merge(
            silver_data.alias("new"),
            "silver.id = new.id"
        ).whenMatchedDelete(
            condition="new.__deleted = 'true'"
        ).whenMatchedUpdate(
            condition="new.__deleted IS NULL OR new.__deleted != 'true'",
            set={
                "employee_id": "new.employee_id",
                "start_datetime": "new.start_datetime",
                "sport_type": "new.sport_type",
                "distance": "new.distance",
                "elapsed_time": "new.elapsed_time",
                "details": "new.details",
                "is_commute": "new.is_commute",
                "updated_at": "new.updated_at"
            }
        ).whenNotMatchedInsert(
            condition="new.__deleted IS NULL OR new.__deleted != 'true'",
            values={
                "id": "new.id",
                "employee_id": "new.employee_id",
                "start_datetime": "new.start_datetime",
                "sport_type": "new.sport_type",
                "distance": "new.distance",
                "elapsed_time": "new.elapsed_time",
                "details": "new.details",
                "is_commute": "new.is_commute",
                "updated_at": "new.updated_at"
            }
        ).execute()

        result_count = spark.read.format("delta").load(SILVER_ACTIVITIES_PATH).count()
        print(f"  → Silver activities (MERGE) : {result_count} activités → {SILVER_ACTIVITIES_PATH}")

    except Exception:
        # Premier lancement : la table Silver n'existe pas encore
        active_data = silver_data.filter(
            col("__deleted").isNull() | (col("__deleted") != "true")
        ).drop("__deleted")

        active_data.write.format("delta") \
            .mode("overwrite") \
            .save(SILVER_ACTIVITIES_PATH)

        result_count = active_data.count()
        print(f"  → Silver activities (initial) : {result_count} activités → {SILVER_ACTIVITIES_PATH}")

    return result_count


# ============================================================
# GOLD LAYER
# ============================================================

def process_gold(spark):
    """Gold : calcul de l'éligibilité Prime sportive & Journées bien-être"""
    print("\n" + "=" * 60)
    print("GOLD - Éligibilité des employés")
    print("=" * 60)

    # Lecture des tables Silver
    try:
        activities = spark.read.format("delta").load(SILVER_ACTIVITIES_PATH)
        employees = spark.read.format("delta").load(SILVER_EMPLOYEES_PATH)
    except Exception as e:
        print(f"  Données Silver manquantes : {e}")
        return 0

    # Statistiques d'activités par employé
    activity_stats = activities.groupBy("employee_id").agg(
        count("*").alias("total_activities"),
        spark_sum(when(col("is_commute") == True, 1).otherwise(0))
            .alias("total_commute_activities"),
        spark_sum(when(col("is_commute") == False, 1).otherwise(0))
            .alias("total_external_activities"),
        spark_sum(spark_coalesce(col("distance"), lit(0)))
            .alias("total_distance_m"),
        spark_sum(spark_coalesce(col("elapsed_time"), lit(0)))
            .alias("total_elapsed_time_s")
    )

    # Jointure avec les données de référence employés
    gold_df = employees.join(activity_stats, on="employee_id", how="left") \
        .fillna(0, subset=[
            "total_activities", "total_commute_activities",
            "total_external_activities", "total_distance_m", "total_elapsed_time_s"
        ])

    # ── Règle 1 : Prime sportive (5% du salaire brut) ──
    # Éligible si :
    #   - Le mode de déplacement déclaré est sportif (Marche/running ou Vélo/Trottinette/Autres)
    #   - L'employé a effectivement des activités de déplacement sportif (preuves)
    gold_df = gold_df.withColumn(
        "is_eligible_prime_sportive",
        when(
            (col("transport_mode").isin(SPORT_TRANSPORT_MODES)) &
            (col("total_commute_activities") > 0),
            lit(True)
        ).otherwise(lit(False))
    ).withColumn(
        "prime_sportive_montant",
        when(
            col("is_eligible_prime_sportive") == True,
            col("salaire_brut") * 0.05
        ).otherwise(lit(0.0))
    )

    # ── Règle 2 : 5 journées bien-être ──
    # Éligible si : au minimum 15 activités physiques externes dans l'année
    gold_df = gold_df.withColumn(
        "is_eligible_journees_bien_etre",
        when(
            col("total_external_activities") >= MIN_ACTIVITIES_WELLNESS,
            lit(True)
        ).otherwise(lit(False))
    ).withColumn("computed_at", current_timestamp())

    # Sélection des colonnes finales pour Gold
    gold_final = gold_df.select(
        "employee_id",
        "nom",
        "prenom",
        "bu",
        "type_de_contrat",
        "transport_mode",
        "external_sport",
        "salaire_brut",
        "total_activities",
        "total_commute_activities",
        "total_external_activities",
        "total_distance_m",
        "total_elapsed_time_s",
        "is_eligible_prime_sportive",
        "prime_sportive_montant",
        "is_eligible_journees_bien_etre",
        "computed_at"
    )

    # Écriture dans Gold Delta Lake
    gold_final.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_ELIGIBILITY_PATH)

    # ── Export Parquet pour Power BI Desktop ──
    gold_final.coalesce(1).write \
        .mode("overwrite") \
        .parquet(POWERBI_ELIGIBILITY)

    # Export des activités Silver en Parquet pour détail Power BI
    activities.coalesce(1).write \
        .mode("overwrite") \
        .parquet(POWERBI_ACTIVITIES)

    # Résumé
    gold_count = gold_final.count()
    eligible_prime = gold_final.filter(col("is_eligible_prime_sportive") == True).count()
    eligible_wellness = gold_final.filter(col("is_eligible_journees_bien_etre") == True).count()

    print(f"  → {gold_count} employés traités → {GOLD_ELIGIBILITY_PATH}")
    print(f"  ├── Éligibles prime sportive (5% salaire)  : {eligible_prime}")
    print(f"  ├── Éligibles journées bien-être (≥15 act) : {eligible_wellness}")
    print(f"  └── Parquet Power BI : {POWERBI_ELIGIBILITY}")
    print(f"                        {POWERBI_ACTIVITIES}")

    return gold_count


# ============================================================
# PIPELINE
# ============================================================

def run_medallion(spark):
    """Exécute le pipeline complet Bronze → Silver → Gold"""
    print("\n" + "#" * 60)
    print("#  MEDALLION PIPELINE — Bronze → Silver → Gold")
    print("#" * 60)

    process_silver_employees(spark)
    process_silver_activities(spark)
    process_gold(spark)

    print("\n" + "#" * 60)
    print("#  PIPELINE TERMINÉ")
    print("#" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Medallion Processor : Bronze → Silver → Gold")
    parser.add_argument("--mode", choices=["single", "watch"], default="single",
                        help="single : traitement unique | watch : traitement continu")
    parser.add_argument("--interval", type=int, default=60,
                        help="Intervalle en secondes entre les traitements (mode watch)")
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        if args.mode == "single":
            run_medallion(spark)
        else:
            print(f"Mode watch : traitement toutes les {args.interval}s (Ctrl+C pour arrêter)")
            while True:
                run_medallion(spark)
                print(f"\nProchain traitement dans {args.interval}s...")
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nArrêt du processeur Medallion.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
