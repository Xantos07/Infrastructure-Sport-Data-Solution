"""Tests unitaires de la logique métier du pipeline Medallion.

Structure identique à generate_ticket/unit_test/ :
  - les fonctions testées sont importées depuis le code de production
  - aucune logique n'est réimplémentée dans ce fichier
  - un changement dans business_rules.py fait échouer les tests concernés

Couverture :
  - normalize_column_name          (services/services.py)
  - is_commute                     (services/business_rules.py)
  - is_active_cdc                  (services/business_rules.py)
  - validate_record                (services/business_rules.py)
  - is_eligible_prime_sportive     (services/business_rules.py)
  - is_eligible_journees_bien_etre (services/business_rules.py)
  - SPORT_TRANSPORT_MODES          (medallion_processor/gold_processor.py)
  - MIN_ACTIVITIES_WELLNESS        (medallion_processor/gold_processor.py)
"""

import sys
from pathlib import Path
# --- Résolution des chemins d'import ----------------------------------------
_SPARK_JOB = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _SPARK_JOB.parent
for _p in [str(_SPARK_JOB), str(_PROJECT_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# --- Imports depuis le code de production ------------------------------------
from services.services import normalize_column_name
from services.business_rules import (
    COMMUTE_PREFIX,
    DISTANCE_LIMITS_BY_SPORT,
    is_commute,
    is_active_cdc,
    validate_record,
    is_eligible_prime_sportive,
    is_eligible_journees_bien_etre,
)
from medallion_processor.gold_processor import SPORT_TRANSPORT_MODES, MIN_ACTIVITIES_WELLNESS


# ============================================================
# 1. Normalisation des colonnes CSV
# ============================================================

class TestNormalizeColumnName:
    """services.services.normalize_column_name"""

    def test_id_salarie(self):
        assert normalize_column_name("ID salarié") == "id_salarie"

    def test_moyen_deplacement(self):
        assert normalize_column_name("Moyen de déplacement") == "moyen_de_deplacement"

    def test_pratique_sport(self):
        assert normalize_column_name("Pratique d'un sport") == "pratique_d_un_sport"

    def test_date_naissance(self):
        assert normalize_column_name("Date de naissance") == "date_de_naissance"

    def test_salaire_brut(self):
        assert normalize_column_name("Salaire brut") == "salaire_brut"

    def test_date_embauche(self):
        assert normalize_column_name("Date d'embauche") == "date_d_embauche"


# ============================================================
# 2. Silver — règle is_commute
# ============================================================

class TestIsCommute:
    """services.business_rules.is_commute — utilise COMMUTE_PREFIX importé."""

    def test_marche(self):
        assert is_commute(f"{COMMUTE_PREFIX} - Marche") is True

    def test_velo(self):
        assert is_commute(f"{COMMUTE_PREFIX} - Vélo") is True

    def test_prefix_seul(self):
        assert is_commute(COMMUTE_PREFIX) is True

    def test_running_non_commute(self):
        assert is_commute("Running") is False

    def test_natation_non_commute(self):
        assert is_commute("Natation") is False

    def test_randonnee_non_commute(self):
        assert is_commute("Randonnée") is False

    def test_none_non_commute(self):
        assert is_commute(None) is False

    def test_empty_string_non_commute(self):
        assert is_commute("") is False


# ============================================================
# 3. Silver — filtrage CDC (__deleted Debezium)
# ============================================================

class TestCDCDeleteHandling:
    """services.business_rules.is_active_cdc"""

    def test_none_is_active(self):
        assert is_active_cdc(None) is True

    def test_false_string_is_active(self):
        assert is_active_cdc("false") is True

    def test_true_string_is_deleted(self):
        assert is_active_cdc("true") is False

    def test_filters_mix_of_records(self):
        records = [
            {"id": 1, "__deleted": None},
            {"id": 2, "__deleted": "true"},
            {"id": 3, "__deleted": "false"},
        ]
        active_ids = [r["id"] for r in records if is_active_cdc(r["__deleted"])]
        assert active_ids == [1, 3]

    def test_all_deleted_returns_empty(self):
        records = [{"id": i, "__deleted": "true"} for i in range(5)]
        assert [r for r in records if is_active_cdc(r["__deleted"])] == []


# ============================================================
# 4. Silver — validation qualité (DataQualityProcessor)
# ============================================================

class TestValidateRecord:
    """services.business_rules.validate_record — miroir de DataQualityProcessor.validate()"""

    def test_valid_record_has_no_violations(self):
        record = {
            "id": 1,
            "employee_id": 10,
            "sport_type": "Running",
            "elapsed_time": 1800,
            "start_datetime": "2026-01-01 08:00",
            "distance": 5000,
        }
        assert validate_record(record) == []

    def test_id_null_detected(self):
        record = {
            "id": None, "employee_id": 10, "sport_type": "Running",
            "elapsed_time": 1800, "start_datetime": "2026-01-01", "distance": 5000,
        }
        assert "id_null" in validate_record(record)

    def test_employee_id_null_detected(self):
        record = {
            "id": 1, "employee_id": None, "sport_type": "Running",
            "elapsed_time": 1800, "start_datetime": "2026-01-01", "distance": 5000,
        }
        assert "employee_id_null" in validate_record(record)

    def test_sport_type_null_detected(self):
        record = {
            "id": 1, "employee_id": 10, "sport_type": None,
            "elapsed_time": 1800, "start_datetime": "2026-01-01", "distance": 5000,
        }
        assert "sport_type_null" in validate_record(record)

    def test_sport_type_empty_string_detected(self):
        record = {
            "id": 1, "employee_id": 10, "sport_type": "",
            "elapsed_time": 1800, "start_datetime": "2026-01-01", "distance": None,
        }
        assert "sport_type_null" in validate_record(record)

    def test_elapsed_time_zero_detected(self):
        record = {
            "id": 1, "employee_id": 10, "sport_type": "Running",
            "elapsed_time": 0, "start_datetime": "2026-01-01", "distance": 5000,
        }
        assert "elapsed_time_invalid" in validate_record(record)

    def test_elapsed_time_negative_detected(self):
        record = {
            "id": 1, "employee_id": 10, "sport_type": "Running",
            "elapsed_time": -300, "start_datetime": "2026-01-01", "distance": 5000,
        }
        assert "elapsed_time_invalid" in validate_record(record)

    def test_elapsed_time_null_detected(self):
        record = {
            "id": 1, "employee_id": 10, "sport_type": "Running",
            "elapsed_time": None, "start_datetime": "2026-01-01", "distance": 5000,
        }
        assert "elapsed_time_invalid" in validate_record(record)

    def test_start_datetime_null_detected(self):
        record = {
            "id": 1, "employee_id": 10, "sport_type": "Running",
            "elapsed_time": 1800, "start_datetime": None, "distance": 5000,
        }
        assert "start_datetime_null" in validate_record(record)

    def test_distance_negative_detected(self):
        record = {
            "id": 1, "employee_id": 10, "sport_type": "Running",
            "elapsed_time": 1800, "start_datetime": "2026-01-01", "distance": -100,
        }
        assert "distance_negative" in validate_record(record)

    def test_distance_none_is_valid(self):
        """None est autorisé pour les sports sans distance mesurable."""
        record = {
            "id": 1, "employee_id": 10, "sport_type": "Tennis",
            "elapsed_time": 3600, "start_datetime": "2026-01-01", "distance": None,
        }
        assert validate_record(record) == []

    def test_distance_outlier_detected_per_sport(self):
        """Distance > seuil pour chaque sport dans DISTANCE_LIMITS_BY_SPORT → outlier détecté."""
        for sport_key, max_dist in DISTANCE_LIMITS_BY_SPORT.items():
            label = "distance_outlier_" + sport_key.split("/")[0].lower().replace(" ", "_")
            record = {
                "id": 1, "employee_id": 10,
                "sport_type": f"{COMMUTE_PREFIX} - {sport_key}",
                "elapsed_time": 3600, "start_datetime": "2026-01-01",
                "distance": max_dist + 1,
            }
            assert label in validate_record(record), f"Outlier non détecté pour {sport_key}"

    def test_distance_at_limit_is_valid(self):
        """Distance exactement au seuil → pas d'outlier (seuil exclu)."""
        for sport_key, max_dist in DISTANCE_LIMITS_BY_SPORT.items():
            record = {
                "id": 1, "employee_id": 10,
                "sport_type": f"{COMMUTE_PREFIX} - {sport_key}",
                "elapsed_time": 3600, "start_datetime": "2026-01-01",
                "distance": max_dist,
            }
            violations = validate_record(record)
            outlier_violations = [v for v in violations if v.startswith("distance_outlier")]
            assert outlier_violations == [], f"Faux positif au seuil exact pour {sport_key}"

    def test_distance_outlier_not_triggered_for_other_sports(self):
        """Un sport sans seuil défini ne déclenche pas d'outlier, quelle que soit la distance."""
        record = {
            "id": 1, "employee_id": 10, "sport_type": "Natation",
            "elapsed_time": 3600, "start_datetime": "2026-01-01", "distance": 999_999,
        }
        outlier_violations = [v for v in validate_record(record) if v.startswith("distance_outlier")]
        assert outlier_violations == []

    def test_multiple_violations_all_reported(self):
        """Un record peut avoir plusieurs violations simultanées."""
        record = {
            "id": None, "employee_id": None, "sport_type": None,
            "elapsed_time": 0, "start_datetime": None, "distance": -1,
        }
        violations = validate_record(record)
        assert "id_null" in violations
        assert "employee_id_null" in violations
        assert "sport_type_null" in violations
        assert "elapsed_time_invalid" in violations
        assert "start_datetime_null" in violations
        assert "distance_negative" in violations


# ============================================================
# 5. Gold — règles d'éligibilité
# ============================================================

class TestEligibilityRules:
    """services.business_rules — constantes importées depuis gold_processor."""

    # --- Prime sportive ---

    def test_prime_eligible_marche(self):
        assert is_eligible_prime_sportive("Marche/running", 5) is True

    def test_prime_eligible_velo(self):
        assert is_eligible_prime_sportive("Vélo/Trottinette/Autres", 1) is True

    def test_prime_ineligible_wrong_transport(self):
        assert is_eligible_prime_sportive("Transports en commun", 3) is False

    def test_prime_ineligible_vehicule_thermique(self):
        assert is_eligible_prime_sportive("véhicule thermique/électrique", 10) is False

    def test_prime_ineligible_zero_commute(self):
        assert is_eligible_prime_sportive("Marche/running", 0) is False

    def test_prime_ineligible_none_transport(self):
        assert is_eligible_prime_sportive(None, 5) is False

    # --- Journées bien-être ---

    def test_wellness_eligible_above_threshold(self):
        assert is_eligible_journees_bien_etre(MIN_ACTIVITIES_WELLNESS + 1) is True

    def test_wellness_eligible_at_threshold(self):
        assert is_eligible_journees_bien_etre(MIN_ACTIVITIES_WELLNESS) is True

    def test_wellness_ineligible_below_threshold(self):
        assert is_eligible_journees_bien_etre(MIN_ACTIVITIES_WELLNESS - 1) is False

    def test_wellness_ineligible_zero(self):
        assert is_eligible_journees_bien_etre(0) is False

    # --- Double éligibilité ---

    def test_both_eligible(self):
        assert is_eligible_prime_sportive("Marche/running", 5) is True
        assert is_eligible_journees_bien_etre(MIN_ACTIVITIES_WELLNESS) is True

    def test_neither_eligible(self):
        assert is_eligible_prime_sportive("Transports en commun", 0) is False
        assert is_eligible_journees_bien_etre(3) is False

    # --- Cohérence des constantes avec le code de production ---

    def test_transport_modes_contient_marche(self):
        assert "Marche/running" in SPORT_TRANSPORT_MODES

    def test_transport_modes_contient_velo(self):
        assert "Vélo/Trottinette/Autres" in SPORT_TRANSPORT_MODES

    def test_seuil_bien_etre_est_15(self):
        assert MIN_ACTIVITIES_WELLNESS == 15
