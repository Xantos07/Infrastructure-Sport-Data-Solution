"""
DATA QUALITY PROCESSOR — Validation et quarantaine avant Silver

Sépare les données Bronze en deux flux :
  ✅ df_clean      : enregistrements valides  → Silver (transform + MERGE)
  ❌ df_quarantine : enregistrements invalides → Delta quarantine + Parquet

Règles de validation appliquées :
  1. id null                   → impossible de dédupliquer ou merger
  2. employee_id null          → impossible de joindre avec les employés
  3. sport_type null ou vide   → is_commute non calculable
  4. elapsed_time null ou <= 0 → métrique fondamentale du ticket
  5. start_datetime null       → impossible de situer l'activité dans le temps
  6. distance négative         → valeur physiquement impossible
  7. employee_id orphelin      → aucune correspondance dans la table de référence
     (optionnel — nécessite la table Silver employees existante)

La colonne rejection_reason liste toutes les règles violées (concat_ws ignore les NULL).
Chaque record peut violer plusieurs règles simultanément.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws, current_timestamp
from delta.tables import DeltaTable

from medallion_processor.medallion_layer import MedallionLayer
from config.configuration import SparkSettings
from services.business_rules import DISTANCE_LIMITS_BY_SPORT


def _outlier_label(sport_key: str) -> str:
    """Génère un label unique à partir de la clé sport (ex: 'Marche/running' → 'distance_outlier_marche')."""
    return "distance_outlier_" + sport_key.split("/")[0].lower().replace(" ", "_")


# ── Règles : (label_lisible, condition_d_invalidité) ─────────────────────────
# Règles structurelles codées ici ; règles outlier générées depuis
# DISTANCE_LIMITS_BY_SPORT (business_rules.py) pour rester synchronisées.
_STRUCTURAL_RULES: list[tuple[str, object]] = [
    ("id_null",              col("id").isNull()),
    ("employee_id_null",     col("employee_id").isNull()),
    ("sport_type_null",      col("sport_type").isNull() | (col("sport_type") == "")),
    ("elapsed_time_invalid", col("elapsed_time").isNull() | (col("elapsed_time") <= 0)),
    ("start_datetime_null",  col("start_datetime").isNull()),
    ("distance_negative",    col("distance").isNotNull() & (col("distance") < 0)),
]

_OUTLIER_RULES: list[tuple[str, object]] = [
    (
        _outlier_label(sport_key),
        col("distance").isNotNull()
        & (col("distance") > max_dist)
        & col("sport_type").contains(sport_key),
    )
    for sport_key, max_dist in DISTANCE_LIMITS_BY_SPORT.items()
]

VALIDATION_RULES: list[tuple[str, object]] = _STRUCTURAL_RULES + _OUTLIER_RULES

class DataQualityProcessor(MedallionLayer):
    """Valide les données Bronze et produit un flux clean + un flux quarantaine."""

    def __init__(self, spark_session: SparkSession, settings: SparkSettings):
        super().__init__(spark_session, settings)

    # ── Validation structurelle ───────────────────────────────────────────────

    def validate(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Évalue toutes les règles et sépare en deux DataFrames.

        concat_ws ignore les valeurs NULL → rejection_reason = ""
        si aucune règle n'est violée, sinon "rule1, rule2, ...".

        Returns:
            df_clean      : lignes sans violation (rejection_reason == "")
            df_quarantine : lignes avec ≥ 1 violation + colonne rejection_reason
                            + colonne rejected_at (timestamp)
        """
        rejection_reason = concat_ws(
            ", ",
            *[when(condition, lit(name)) for name, condition in VALIDATION_RULES],
        )

        df_tagged = df.withColumn("rejection_reason", rejection_reason)

        df_clean = (
            df_tagged
            .filter(col("rejection_reason") == "")
            .drop("rejection_reason")
        )

        df_quarantine = (
            df_tagged
            .filter(col("rejection_reason") != "")
            .withColumn("rejected_at", current_timestamp())
        )

        return df_clean, df_quarantine

    # ── Vérification d'intégrité référentielle (optionnelle) ─────────────────

    def check_referential_integrity(
        self, df_clean: DataFrame
    ) -> tuple[DataFrame, DataFrame]:
        """
        Vérifie que chaque employee_id du flux clean existe dans Silver employees.
        Appelé uniquement si la table Silver employees existe déjà.

        Returns:
            df_valid   : employee_id connu
            df_orphans : employee_id inconnu → quarantaine avec reason "employee_id_not_in_reference"
        """
        if not DeltaTable.isDeltaTable(self.spark, self.settings.delta_silver_employees):
            print("  ℹ  Intégrité référentielle ignorée : Silver employees inexistante.")
            return df_clean, self.spark.createDataFrame([], df_clean.schema)

        known_ids = (
            self.spark.read.format("delta")
            .load(self.settings.delta_silver_employees)
            .select(col("employee_id").alias("_known_id"))
            .distinct()
        )

        df_joined = df_clean.join(known_ids, df_clean["employee_id"] == col("_known_id"), "left")

        df_valid = (
            df_joined
            .filter(col("_known_id").isNotNull())
            .drop("_known_id")
        )

        df_orphans = (
            df_joined
            .filter(col("_known_id").isNull())
            .drop("_known_id")
            .withColumn("rejection_reason", lit("employee_id_not_in_reference"))
            .withColumn("rejected_at", current_timestamp())
        )

        return df_valid, df_orphans

    # ── Écriture quarantaine ──────────────────────────────────────────────────

    def write_quarantine(self, df: DataFrame) -> None:
        """
        Écrit les enregistrements invalides en mode append (historique conservé).
        Delta Lake : pour requêtes et time travel.
        Parquet     : pour investigation externe (Power BI, Excel).
        Ne fait rien si le DataFrame est vide.
        """
        count = df.count()
        if count == 0:
            print("  ✅ Quarantaine : aucun enregistrement invalide détecté.")
            return

        print(f"\n  {'='*10} QUARANTAINE — {count} enregistrement(s) rejeté(s) {'='*10}")

        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(self.settings.delta_quarantine_activities)

        df.write \
            .mode("append") \
            .parquet(self.settings.powerbi_quarantine)

        self._print_rejection_summary(df)

    # ── Rapport ───────────────────────────────────────────────────────────────

    def _print_rejection_summary(self, df: DataFrame) -> None:
        """Décompose rejection_reason et affiche le comptage par règle."""
        from pyspark.sql.functions import explode, split

        summary = (
            df.select(explode(split(col("rejection_reason"), ", ")).alias("rule"))
            .groupBy("rule")
            .count()
            .orderBy("count", ascending=False)
            .collect()
        )

        total = df.count()
        print(f"  Détail des violations ({total} records) :")
        for i, row in enumerate(summary):
            connector = "└──" if i == len(summary) - 1 else "├──"
            print(f"    {connector} {row['rule']:<40} : {row['count']} record(s)")
