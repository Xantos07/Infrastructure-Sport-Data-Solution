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
from config.configuration import SparkSettings
from medallion_processor.silver_activity_processor import SilverActivityProcessor
from medallion_processor.silver_employees_processor import SilverEmployeesProcessor
from medallion_processor.gold_processor import GoldProcessor
from services.bronze_monitor import BronzeMonitor

spark_settings = SparkSettings()
BRONZE_PATH = spark_settings.delta_bronze_path

# ============================================================
# PIPELINE
# ============================================================

# mettre une logique de nettoyage interne à silver
# c'est generate ticket qui nettoie mais si on change de source de données
# on se retrouvera avec des données sales dans bronze puis silver

# a refacto pas tres propre
_last_bronze_count: int = -2  # sentinel : jamais traité


def run_medallion(spark, monitor):
    global _last_bronze_count
    print("\n" + "#" * 60)
    print("#  MEDALLION PIPELINE — Bronze → Silver → Gold")
    print("#" * 60)

    current_count = monitor.get_bronze_count()

    if current_count < 0:
        print("  Bronze inexistante ou vide — pipeline ignoré.")
        print("#" * 60)
        return

    if current_count == _last_bronze_count:
        print(f"  Aucun nouveau ticket Bronze ({current_count} lignes, inchangé) — pipeline ignoré.")
        print("#" * 60)
        return

    print(f"  Bronze : {_last_bronze_count if _last_bronze_count >= 0 else '?'} → {current_count} lignes")
    SilverActivityProcessor(spark, spark_settings).run()
    SilverEmployeesProcessor(spark, spark_settings).run()
    GoldProcessor(spark, spark_settings).run()
    _last_bronze_count = current_count
    print("\n" + "#" * 60)
    print("#  PIPELINE TERMINÉ")
    print("#" * 60)

def main():
    parser = argparse.ArgumentParser(description="Medallion Processor : Bronze → Silver → Gold")
    parser.add_argument("--mode", choices=["single", "watch"], default="single")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--wait-bronze", action="store_true")
    parser.add_argument("--bronze-wait-timeout", type=int, default=900)
    parser.add_argument("--bronze-wait-interval", type=int, default=5)
    args = parser.parse_args()

    spark = SparkSession.builder \
    .appName("Medallion Processor - Silver/Gold") \
    .getOrCreate()

    monitor = BronzeMonitor(spark, BRONZE_PATH)
    
    try:
        if args.mode == "single":
            run_medallion(spark, monitor)
        else:
            if args.wait_bronze:
                ready = monitor.wait_for_bronze_data(
                    timeout_seconds=args.bronze_wait_timeout,
                    interval_seconds=args.bronze_wait_interval,
                )
                if not ready:
                    raise RuntimeError("Bronze non prête avant timeout; arrêt du job medallion.")

            print(f"Mode watch : traitement toutes les {args.interval}s (Ctrl+C pour arrêter)")
            while True:
                run_medallion(spark, monitor)
                print(f"\nProchain traitement dans {args.interval}s...")
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nArrêt du processeur Medallion.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()