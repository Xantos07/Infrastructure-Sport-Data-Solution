"""
Règles métier du pipeline Medallion — source de vérité unique.

Ce module est importé par :
  - les processeurs Spark  (silver_activity_processor, data_quality_processor)
  - les tests unitaires    (test_medallion_processor)

Modifier une règle ou un seuil ici le propage automatiquement partout.
Les tests détectent immédiatement toute incohérence.
"""

from __future__ import annotations

from medallion_processor.gold_processor import SPORT_TRANSPORT_MODES, MIN_ACTIVITIES_WELLNESS


# ── Constantes Silver ─────────────────────────────────────────────────────────

COMMUTE_PREFIX = "Commute to work"

# Distance maximale acceptable par mode de déplacement (en mètres).
# Clé = sous-chaîne contenue dans sport_type (ex: "Commute to work - Marche/running").
# Importée par DataQualityProcessor pour construire les VALIDATION_RULES Spark.
DISTANCE_LIMITS_BY_SPORT = {
    "Marche/running":          15_000,   # 15 km — au-delà = suspicion de saisie erronée
    "Vélo/Trottinette/Autres": 25_000,   # 25 km — au-delà = suspicion de saisie erronée
}


# ── Silver : règle is_commute ─────────────────────────────────────────────────

def is_commute(sport_type: str | None) -> bool:
    """
    True si l'activité est un déplacement domicile-travail sportif.

    Miroir Spark dans silver_activity_processor.transform() :
        col("sport_type").startswith(COMMUTE_PREFIX)
    """
    return bool(sport_type and sport_type.startswith(COMMUTE_PREFIX))


# ── Silver : règle CDC __deleted ──────────────────────────────────────────────

def is_active_cdc(deleted_flag: str | None) -> bool:
    """
    True si l'enregistrement n'est pas marqué supprimé par Debezium.

    Miroir Spark dans silver_activity_processor.write_silver() :
        col("__deleted").isNull() | (col("__deleted") != "true")
    """
    return deleted_flag is None or deleted_flag != "true"


# ── Silver : règles de validation qualité ────────────────────────────────────

def validate_record(record: dict) -> list[str]:
    """
    Évalue les règles de validation sur un enregistrement Bronze.
    Retourne la liste des violations (liste vide = enregistrement valide).

    Miroir Spark dans DataQualityProcessor (VALIDATION_RULES).
    Les deux utilisent les mêmes constantes — ils restent synchronisés.
    """
    violations = []
    sport_type = record.get("sport_type")
    distance   = record.get("distance")

    # ── Règles structurelles ──────────────────────────────────────────────────
    if record.get("id") is None:
        violations.append("id_null")

    if record.get("employee_id") is None:
        violations.append("employee_id_null")

    if sport_type is None or sport_type == "":
        violations.append("sport_type_null")

    elapsed_time = record.get("elapsed_time")
    if elapsed_time is None or elapsed_time <= 0:
        violations.append("elapsed_time_invalid")

    if record.get("start_datetime") is None:
        violations.append("start_datetime_null")

    if distance is not None and distance < 0:
        violations.append("distance_negative")

    # ── Règles outlier (seuils importés de DISTANCE_LIMITS_BY_SPORT) ─────────
    if distance is not None and sport_type:
        for sport_key, max_dist in DISTANCE_LIMITS_BY_SPORT.items():
            if sport_key in sport_type and distance > max_dist:
                label = "distance_outlier_" + sport_key.split("/")[0].lower().replace(" ", "_")
                violations.append(label)

    return violations


# ── Gold : règles d'éligibilité ───────────────────────────────────────────────

def is_eligible_prime_sportive(transport_mode: str | None, total_commute_activities: int) -> bool:
    """
    True si éligible à la prime sportive.
    Conditions : transport sportif + au moins 1 activité de trajet.

    Miroir Spark dans GoldProcessor.prime_sportive_rule() :
        col("transport_mode").isin(SPORT_TRANSPORT_MODES) & (col("total_commute_activities") > 0)
    """
    return transport_mode in SPORT_TRANSPORT_MODES and total_commute_activities > 0


def is_eligible_journees_bien_etre(total_external_activities: int) -> bool:
    """
    True si éligible aux journées bien-être.
    Condition : activités externes >= MIN_ACTIVITIES_WELLNESS.

    Miroir Spark dans GoldProcessor.prime_sportive_rule() :
        col("total_external_activities") >= MIN_ACTIVITIES_WELLNESS
    """
    return total_external_activities >= MIN_ACTIVITIES_WELLNESS
