"""
MEDALLION PROCESSOR - Bronze → Silver → Gold

Traite les données du Delta Lake selon l'architecture Medallion :
  Bronze : Données brutes CDC (append-only) depuis Kafka
  Silver : Données nettoyées, dédupliquées (MERGE) + table de référence employés
  Gold   : Métriques métier — Éligibilité Prime sportive & Journées bien-être

  ┌─────────────────────────────────────────────────────────┐
  │  Responsabilités par couche                             │
  │  Silver → faits bruts structurés  (1 ticket = 1 ligne) │
  │  Gold   → agrégats + éligibilité  (1 employé = 1 ligne) │
  │  Power BI → calculs paramétrables (taux, seuils, KPIs)  │
  └─────────────────────────────────────────────────────────┘

Stockage : MinIO (S3-compatible)
  Delta Lake : s3a://delta-lake/
  Parquet PBI : s3a://powerbi/
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
from config.configuration import SparkSettings

# ============================================================
# Chemins Delta Lake — MinIO (s3a://)
# ============================================================
spark_settings = SparkSettings()
BRONZE_PATH            = spark_settings.delta_bronze_path

SILVER_ACTIVITIES_PATH = spark_settings.delta_silver_activities
SILVER_EMPLOYEES_PATH  = spark_settings.delta_silver_employees

GOLD_ELIGIBILITY_PATH  = spark_settings.delta_gold_eligibility

# ============================================================
# Sorties Parquet pour Power BI Desktop — MinIO (s3a://)
# S3A gère l'overwrite proprement, pas besoin de suppression manuelle
# ============================================================
POWERBI_ELIGIBILITY = spark_settings.powerbi_eligibility
POWERBI_ACTIVITIES  = spark_settings.powerbi_activities

# ============================================================
# Données de référence (CSV montés via volume Docker)
# ============================================================
REF_EMPLOYEES_CSV = spark_settings.delta_input_employees
REF_SPORTS_CSV    = spark_settings.delta_input_sports
# ============================================================
# Règles métier — seuils d'éligibilité (stables, pas de taux)
# Les taux financiers (ex: 5% prime) sont gérés dans Power BI
# via What-If Parameters pour pouvoir les modifier en démo.
# ============================================================
SPORT_TRANSPORT_MODES   = ["Marche/running", "Vélo/Trottinette/Autres"]
MIN_ACTIVITIES_WELLNESS = 15


def create_spark_session():
    """Crée une session Spark avec support Delta Lake + MinIO"""

    return SparkSession.builder \
        .appName("Medallion Processor - Bronze/Silver/Gold") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", spark_settings.minio_base_url) \
        .config("spark.hadoop.fs.s3a.access.key", spark_settings.minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", spark_settings.minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


# ============================================================
# SILVER LAYER
# ============================================================

def process_silver_employees(spark):
    print("\n" + "=" * 60)
    print("SILVER - Données de référence employés")
    print("=" * 60)

    # Lecture avec schema explicite — le BOM UTF-8 sur "ID salarié" est gere
    # par la lecture header=True qui normalise les noms de colonnes

    df_employees = spark.read.csv(REF_EMPLOYEES_CSV, header=True, inferSchema=True, encoding="UTF-8")
    df_sports    = spark.read.csv(REF_SPORTS_CSV,    header=True, inferSchema=True, encoding="UTF-8")
    df_ref = df_employees.join(df_sports, on="ID salarié", how="left")

    def normalize_column_name(name):
        import unicodedata
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        name = name.replace(' ', '_').replace("'", '_').replace('é', 'e').replace('è', 'e')
        return name.lower()

    for old_col in df_ref.columns:
        df_ref = df_ref.withColumnRenamed(old_col, normalize_column_name(old_col))

    silver_employees = df_ref \
        .withColumnRenamed("id_salarie",          "employee_id") \
        .withColumnRenamed("moyen_de_deplacement", "transport_mode") \
        .withColumnRenamed("pratique_d_un_sport",  "external_sport") \
        .withColumn("updated_at", current_timestamp())

    # Assertions de qualité
    null_ids = silver_employees.filter(col("employee_id").isNull()).count()
    if null_ids > 0:
        print(f"  ⚠ QUALITÉ : {null_ids} employé(s) avec employee_id NULL détecté(s)")

    silver_employees.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_EMPLOYEES_PATH)

    nb = silver_employees.count()
    assert nb > 0, "Silver employees vide — vérifier les CSV de référence"
    print(f"  → {nb} employés chargés dans {SILVER_EMPLOYEES_PATH}")
    return nb


def process_silver_activities(spark):
    print("\n" + "=" * 60)
    print("SILVER - Traitement Bronze → Silver (activités)")
    print("=" * 60)

    try:
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    except Exception as e:
        print(f"  Aucune donnée Bronze trouvée : {e}")
        return 0

    bronze_count = bronze_df.count()
    if bronze_count == 0:
        print("  Aucune donnée dans la couche Bronze.")
        return 0

    # Assertions de qualité Bronze
    null_ids = bronze_df.filter(col("id").isNull()).count()
    null_employees = bronze_df.filter(col("employee_id").isNull()).count()
    if null_ids > 0:
        print(f"  ⚠ QUALITÉ : {null_ids} enregistrement(s) Bronze avec id NULL")
    if null_employees > 0:
        print(f"  ⚠ QUALITÉ : {null_employees} enregistrement(s) Bronze avec employee_id NULL")

    print(f"  Bronze : {bronze_count} événements CDC")

    window = Window.partitionBy("id").orderBy(desc("ingested_at"))
    latest_df = bronze_df \
        .withColumn("rn", row_number().over(window)) \
        .filter("rn = 1") \
        .drop("rn")

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
    )

    if DeltaTable.isDeltaTable(spark, SILVER_ACTIVITIES_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_ACTIVITIES_PATH)

        silver_table.alias("silver").merge(
            silver_data.alias("new"),
            "silver.id = new.id"
        ).whenMatchedDelete(
            condition="new.__deleted = 'true'"
        ).whenMatchedUpdate(
            # Seulement si une colonne métier a réellement changé — évite les réécritures inutiles
            condition="""
                (new.__deleted IS NULL OR new.__deleted != 'true') AND (
                    silver.employee_id    != new.employee_id    OR
                    silver.start_datetime != new.start_datetime OR
                    silver.sport_type     != new.sport_type     OR
                    silver.distance       != new.distance        OR
                    silver.elapsed_time   != new.elapsed_time    OR
                    silver.details        != new.details          OR
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

        result_count = spark.read.format("delta").load(SILVER_ACTIVITIES_PATH).count()
        print(f"  → Silver activities (MERGE) : {result_count} activités → {SILVER_ACTIVITIES_PATH}")
    else:
        active_data = silver_data.filter(
            col("__deleted").isNull() | (col("__deleted") != "true")
        ).drop("__deleted")

        active_data.write.format("delta").mode("overwrite").save(SILVER_ACTIVITIES_PATH)

        result_count = active_data.count()
        print(f"  → Silver activities (initial) : {result_count} activités → {SILVER_ACTIVITIES_PATH}")

    return result_count


# ============================================================
# GOLD LAYER
# ============================================================

def process_gold(spark):
    print("\n" + "=" * 60)
    print("GOLD - Éligibilité des employés")
    print("=" * 60)

    try:
        activities = spark.read.format("delta").load(SILVER_ACTIVITIES_PATH)
        employees  = spark.read.format("delta").load(SILVER_EMPLOYEES_PATH)
    except Exception as e:
        print(f"  Données Silver manquantes : {e}")
        return 0

    activity_stats = activities.groupBy("employee_id").agg(
        count("*").alias("total_activities"),
        spark_sum(when(col("is_commute") == True, 1).otherwise(0)).alias("total_commute_activities"),
        spark_sum(when(col("is_commute") == False, 1).otherwise(0)).alias("total_external_activities"),
        spark_sum(spark_coalesce(col("distance"),     lit(0))).alias("total_distance_m"),
        spark_sum(spark_coalesce(col("elapsed_time"), lit(0))).alias("total_elapsed_time_s")
    )

    gold_df = employees.join(activity_stats, on="employee_id", how="left") \
        .fillna(0, subset=[
            "total_activities", "total_commute_activities",
            "total_external_activities", "total_distance_m", "total_elapsed_time_s"
        ])

    # Règle 1 : Prime sportive — MONTANT calculé dans Power BI (What-If Parameter)
    gold_df = gold_df.withColumn(
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

    # Écriture Gold Delta Lake (MinIO)
    gold_final.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_ELIGIBILITY_PATH)

    # Export Parquet Power BI (MinIO) — S3A gère l'overwrite, pas de suppression manuelle
    gold_final.write.mode("overwrite").parquet(POWERBI_ELIGIBILITY)
    activities.write.mode("overwrite").parquet(POWERBI_ACTIVITIES)

    gold_count        = gold_final.count()
    eligible_prime    = gold_final.filter(col("is_eligible_prime_sportive") == True).count()
    eligible_wellness = gold_final.filter(col("is_eligible_journees_bien_etre") == True).count()

    print(f"  → {gold_count} employés traités → {GOLD_ELIGIBILITY_PATH}")
    print(f"  ├── Éligibles prime sportive                        : {eligible_prime} / {gold_count}")
    print(f"  ├── Éligibles journées bien-être (≥{MIN_ACTIVITIES_WELLNESS} activités) : {eligible_wellness} / {gold_count}")
    print(f"  └── Parquet Power BI : {POWERBI_ELIGIBILITY}")
    print(f"                        {POWERBI_ACTIVITIES}")
    print(f"  ℹ  Montant prime = salaire_brut × [Taux What-If] — calculé dans Power BI")

    return gold_count


# ============================================================
# PIPELINE
# ============================================================

def get_bronze_count(spark) -> int:
    """Retourne le nombre de lignes dans Bronze, ou -1 si inexistant.

    Delta Lake résout ce count via les statistiques du transaction log
    (pas de scan fichier) — opération rapide.
    """
    try:
        if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
            return -1
        return spark.read.format("delta").load(BRONZE_PATH).count()
    except Exception:
        return -1


_last_bronze_count: int = -2  # sentinel : jamais traité


def run_medallion(spark):
    global _last_bronze_count
    print("\n" + "#" * 60)
    print("#  MEDALLION PIPELINE — Bronze → Silver → Gold")
    print("#" * 60)

    current_count = get_bronze_count(spark)

    if current_count < 0:
        print("  Bronze inexistante ou vide — pipeline ignoré.")
        print("#" * 60)
        return

    if current_count == _last_bronze_count:
        print(f"  Aucun nouveau ticket Bronze ({current_count} lignes, inchangé) — pipeline ignoré.")
        print("#" * 60)
        return

    print(f"  Bronze : {_last_bronze_count if _last_bronze_count >= 0 else '?'} → {current_count} lignes")
    process_silver_employees(spark)
    process_silver_activities(spark)
    process_gold(spark)
    _last_bronze_count = current_count
    print("\n" + "#" * 60)
    print("#  PIPELINE TERMINÉ")
    print("#" * 60)


def bronze_has_data(spark):
    try:
        is_delta = DeltaTable.isDeltaTable(spark, BRONZE_PATH)
        print(f"  [bronze_has_data] isDeltaTable={is_delta} path={BRONZE_PATH}")
        if not is_delta:
            return False
        rows = spark.read.format("delta").load(BRONZE_PATH).take(1)
        print(f"  [bronze_has_data] take(1)={rows}")
        return rows != []
    except Exception as e:
        print(f"  [bronze_has_data] ERREUR: {e}")
        return False


def wait_for_bronze_data(spark, timeout_seconds, interval_seconds):
    print(f"Attente Bronze : path={BRONZE_PATH}")
    deadline = None if timeout_seconds <= 0 else time.time() + timeout_seconds

    while True:
        if bronze_has_data(spark):
            print("Bronze prête : au moins 1 enregistrement détecté.")
            return True

        if deadline is not None and time.time() >= deadline:
            print(f"Timeout Bronze atteint ({timeout_seconds}s) sans donnée.")
            return False

        print(f"Bronze vide/non disponible, nouvelle vérification dans {interval_seconds}s...")
        time.sleep(interval_seconds)


def main():
    parser = argparse.ArgumentParser(description="Medallion Processor : Bronze → Silver → Gold")
    parser.add_argument("--mode", choices=["single", "watch"], default="single")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--wait-bronze", action="store_true")
    parser.add_argument("--bronze-wait-timeout", type=int, default=900)
    parser.add_argument("--bronze-wait-interval", type=int, default=5)
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        if args.mode == "single":
            run_medallion(spark)
        else:
            if args.wait_bronze:
                ready = wait_for_bronze_data(
                    spark,
                    timeout_seconds=args.bronze_wait_timeout,
                    interval_seconds=args.bronze_wait_interval,
                )
                if not ready:
                    raise RuntimeError("Bronze non prête avant timeout; arrêt du job medallion.")

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