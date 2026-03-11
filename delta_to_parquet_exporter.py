"""
Delta Lake Gold → Parquet Exporter pour Power BI Desktop

Exporte les tables Gold du Delta Lake vers des fichiers Parquet accessibles depuis Windows.
Ces tables contiennent les métriques finales : éligibilité Prime sportive & Journées bien-être.

Note : Le spark_medallion_processor.py exporte aussi directement en Parquet.
       Ce script est un outil autonome de secours si besoin de ré-exporter.

Usage (depuis le host Windows) :
  docker exec -it spark-master /opt/spark/bin/spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    /opt/spark/work/delta_to_parquet_exporter.py --mode single

  Modes :
    --mode single   : Export unique puis arrêt
    --mode watch    : Export continu toutes les N secondes (défaut: 30s)
"""

import argparse
import shutil
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Chemins Delta Lake (Gold)
GOLD_ELIGIBILITY_PATH = "/opt/spark/delta/gold/employee_eligibility"
SILVER_ACTIVITIES_PATH = "/opt/spark/delta/silver/activities"

# Sorties Parquet pour Power BI
PARQUET_ELIGIBILITY = "/opt/spark/powerbi_data/employee_eligibility.parquet"
PARQUET_ACTIVITIES = "/opt/spark/powerbi_data/activities.parquet"
PARQUET_TEMP = "/opt/spark/powerbi_data/.export_temp"


def create_spark_session():
    """Crée une session Spark avec support Delta Lake"""
    return SparkSession.builder \
        .appName("Delta to Parquet - Power BI Export") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def export_delta_to_parquet(spark):
    """Lit les tables Gold/Silver du Delta Lake et exporte en Parquet pour Power BI"""
    try:
        success = True

        # ── Export Gold : éligibilité employés ──
        try:
            df_elig = spark.read.format("delta").load(GOLD_ELIGIBILITY_PATH)
            count_elig = df_elig.count()
            if count_elig > 0:
                _atomic_parquet_write(df_elig, PARQUET_ELIGIBILITY)
                print(f"  Export Gold : {count_elig} employés → {PARQUET_ELIGIBILITY}")
            else:
                print("  Gold employee_eligibility : aucune donnée.")
                success = False
        except Exception as e:
            if "is not a Delta table" in str(e) or "doesn't exist" in str(e):
                print("  Aucune table Gold trouvée. Lancez d'abord spark_medallion_processor.py.")
            else:
                print(f"  Erreur Gold : {e}")
            success = False

        # ── Export Silver : activités détaillées ──
        try:
            df_act = spark.read.format("delta").load(SILVER_ACTIVITIES_PATH)
            count_act = df_act.count()
            if count_act > 0:
                _atomic_parquet_write(df_act, PARQUET_ACTIVITIES)
                print(f"  Export Silver : {count_act} activités → {PARQUET_ACTIVITIES}")
            else:
                print("  Silver activities : aucune donnée.")
        except Exception as e:
            if "is not a Delta table" in str(e) or "doesn't exist" in str(e):
                print("  Aucune table Silver activities trouvée.")
            else:
                print(f"  Erreur Silver : {e}")

        return success

    except Exception as e:
        print(f"Erreur lors de l'export : {e}")
        import traceback
        traceback.print_exc()
        return False


def _atomic_parquet_write(df, output_path):
    """Écriture atomique : dossier temporaire puis remplacement"""
    if os.path.exists(PARQUET_TEMP):
        shutil.rmtree(PARQUET_TEMP)
    df.coalesce(1).write.mode("overwrite").parquet(PARQUET_TEMP)
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    os.rename(PARQUET_TEMP, output_path)


def run_single(spark):
    """Export unique"""
    print("=" * 60)
    print("Export Delta Lake Gold → Parquet (mode single)")
    print("=" * 60)
    export_delta_to_parquet(spark)


def run_watch(spark, interval=30):
    """Export continu avec intervalle"""
    print("=" * 60)
    print(f"Export Delta Lake Gold → Parquet (mode watch, intervalle: {interval}s)")
    print("Appuyez sur Ctrl+C pour arrêter.")
    print("=" * 60)
    while True:
        export_delta_to_parquet(spark)
        print(f"Prochain export dans {interval}s...")
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="Export Delta Lake Gold → Parquet pour Power BI")
    parser.add_argument("--mode", choices=["single", "watch"], default="single",
                        help="Mode d'export : single (une fois) ou watch (continu)")
    parser.add_argument("--interval", type=int, default=30,
                        help="Intervalle en secondes entre les exports (mode watch)")
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        if args.mode == "single":
            run_single(spark)
        else:
            run_watch(spark, args.interval)
    except KeyboardInterrupt:
        print("\nArrêt de l'export.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
