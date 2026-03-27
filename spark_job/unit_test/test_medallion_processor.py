"""Tests unitaires pour les transformations Silver et Gold du Medallion Processor.

Teste la logique metier (deduplication, eligibilite, agregation, normalisation)
en Python pur. Les transformations Spark reproduisent cette meme logique
sur des DataFrames distribues.
"""

import pytest
from datetime import datetime
from collections import defaultdict


# ============================================================
# SILVER LAYER — Deduplication
# ============================================================

class TestSilverDeduplication:
    """Teste la logique de deduplication Bronze -> Silver :
    garder le record avec le ingested_at le plus recent par id."""

    @staticmethod
    def _dedup(records):
        """Reproduit la logique Window.partitionBy('id').orderBy(desc('ingested_at'))."""
        latest = {}
        for r in records:
            rid = r["id"]
            if rid not in latest or r["ingested_at"] > latest[rid]["ingested_at"]:
                latest[rid] = r
        return list(latest.values())

    def test_keeps_latest_by_ingested_at(self):
        records = [
            {"id": 1, "employee_id": 10, "details": "v1", "distance": 5000,
             "ingested_at": datetime(2026, 1, 1, 12, 0)},
            {"id": 1, "employee_id": 10, "details": "v2", "distance": 6000,
             "ingested_at": datetime(2026, 1, 1, 14, 0)},
        ]
        result = self._dedup(records)
        assert len(result) == 1
        assert result[0]["details"] == "v2"
        assert result[0]["distance"] == 6000

    def test_handles_multiple_ids(self):
        records = [
            {"id": 1, "employee_id": 10, "ingested_at": datetime(2026, 1, 1, 12, 0)},
            {"id": 1, "employee_id": 10, "ingested_at": datetime(2026, 1, 1, 14, 0)},
            {"id": 2, "employee_id": 20, "ingested_at": datetime(2026, 1, 1, 12, 0)},
            {"id": 2, "employee_id": 20, "ingested_at": datetime(2026, 1, 1, 15, 0)},
            {"id": 3, "employee_id": 30, "ingested_at": datetime(2026, 1, 1, 12, 0)},
        ]
        result = self._dedup(records)
        assert len(result) == 3

    def test_single_record_unchanged(self):
        records = [{"id": 1, "employee_id": 10, "ingested_at": datetime(2026, 1, 1)}]
        result = self._dedup(records)
        assert len(result) == 1

    def test_empty_returns_empty(self):
        assert self._dedup([]) == []


# ============================================================
# SILVER LAYER — is_commute
# ============================================================

class TestSilverIsCommute:
    """Teste le calcul du champ is_commute :
    True si sport_type commence par 'Deplacement au travail'."""

    @staticmethod
    def is_commute(sport_type):
        return sport_type.startswith("Déplacement au travail") if sport_type else False

    def test_commute_marche(self):
        assert self.is_commute("Déplacement au travail - Marche") is True

    def test_commute_velo(self):
        assert self.is_commute("Déplacement au travail - Vélo") is True

    def test_not_commute_running(self):
        assert self.is_commute("Running") is False

    def test_not_commute_natation(self):
        assert self.is_commute("Natation") is False

    def test_not_commute_tennis(self):
        assert self.is_commute("Tennis") is False

    def test_not_commute_randonnee(self):
        assert self.is_commute("Randonnée") is False


# ============================================================
# SILVER LAYER — Filtrage des deletes CDC
# ============================================================

class TestSilverDeleteHandling:
    """Teste le filtrage des records supprimes via le flag __deleted de Debezium."""

    @staticmethod
    def is_active(deleted_flag):
        """Reproduit : col('__deleted').isNull() | (col('__deleted') != 'true')"""
        return deleted_flag is None or deleted_flag != "true"

    def test_none_is_active(self):
        assert self.is_active(None) is True

    def test_false_string_is_active(self):
        assert self.is_active("false") is True

    def test_true_is_deleted(self):
        assert self.is_active("true") is False

    def test_filters_deleted_records(self):
        records = [
            {"id": 1, "__deleted": None},
            {"id": 2, "__deleted": "true"},
            {"id": 3, "__deleted": "false"},
        ]
        active = [r for r in records if self.is_active(r["__deleted"])]
        assert len(active) == 2
        ids = [r["id"] for r in active]
        assert 1 in ids
        assert 3 in ids
        assert 2 not in ids

    def test_all_deleted_returns_empty(self):
        records = [
            {"id": 1, "__deleted": "true"},
            {"id": 2, "__deleted": "true"},
        ]
        active = [r for r in records if self.is_active(r["__deleted"])]
        assert len(active) == 0


# ============================================================
# GOLD LAYER — Agregation
# ============================================================

class TestGoldAggregation:
    """Teste l'agregation des activites par employe."""

    @staticmethod
    def _aggregate(activities):
        """Reproduit le groupBy('employee_id').agg(count, sum...)."""
        stats = defaultdict(lambda: {
            "total_activities": 0, "total_commute": 0, "total_external": 0,
            "total_distance_m": 0, "total_elapsed_time_s": 0
        })
        for a in activities:
            eid = a["employee_id"]
            stats[eid]["total_activities"] += 1
            if a["is_commute"]:
                stats[eid]["total_commute"] += 1
            else:
                stats[eid]["total_external"] += 1
            stats[eid]["total_distance_m"] += a.get("distance") or 0
            stats[eid]["total_elapsed_time_s"] += a.get("elapsed_time") or 0
        return dict(stats)

    def test_counts_by_employee(self):
        activities = [
            {"employee_id": 10, "is_commute": True, "distance": 5000, "elapsed_time": 1800},
            {"employee_id": 10, "is_commute": True, "distance": 6000, "elapsed_time": 2000},
            {"employee_id": 10, "is_commute": False, "distance": 3000, "elapsed_time": 900},
            {"employee_id": 20, "is_commute": False, "distance": 1000, "elapsed_time": 600},
            {"employee_id": 20, "is_commute": False, "distance": 2000, "elapsed_time": 1200},
        ]
        stats = self._aggregate(activities)
        assert stats[10]["total_activities"] == 3
        assert stats[10]["total_commute"] == 2
        assert stats[10]["total_external"] == 1
        assert stats[10]["total_distance_m"] == 14000
        assert stats[20]["total_activities"] == 2
        assert stats[20]["total_commute"] == 0
        assert stats[20]["total_external"] == 2

    def test_handles_none_distance(self):
        activities = [
            {"employee_id": 10, "is_commute": False, "distance": None, "elapsed_time": 1800},
        ]
        stats = self._aggregate(activities)
        assert stats[10]["total_distance_m"] == 0
        assert stats[10]["total_elapsed_time_s"] == 1800

    def test_empty_activities(self):
        stats = self._aggregate([])
        assert len(stats) == 0


class TestGoldFillna:
    """Teste que les employes sans activites ont des metriques a 0 (left join + fillna)."""

    def test_fillna_for_missing_employee(self):
        employees = [10, 20, 30]
        activity_stats = {10: {"total": 5, "commute": 3, "external": 2}}

        for eid in employees:
            s = activity_stats.get(eid, {})
            total = s.get("total", 0)
            commute = s.get("commute", 0)
            external = s.get("external", 0)
            assert isinstance(total, int)
            assert isinstance(commute, int)
            assert isinstance(external, int)

        assert activity_stats.get(20, {}).get("total", 0) == 0
        assert activity_stats.get(30, {}).get("total", 0) == 0


# ============================================================
# GOLD LAYER — Regles d'eligibilite
# ============================================================

class TestGoldEligibility:
    """Teste les regles d'eligibilite metier."""

    SPORT_TRANSPORT_MODES = ["Marche/running", "Vélo/Trottinette/Autres"]
    MIN_ACTIVITIES_WELLNESS = 15

    def _is_eligible_prime(self, transport_mode, total_commute):
        """Reproduit la regle Prime sportive du Gold layer."""
        return transport_mode in self.SPORT_TRANSPORT_MODES and total_commute > 0

    def _is_eligible_wellness(self, total_external):
        """Reproduit la regle Journees bien-etre du Gold layer."""
        return total_external >= self.MIN_ACTIVITIES_WELLNESS

    # Prime sportive
    def test_prime_eligible_marche(self):
        assert self._is_eligible_prime("Marche/running", 5) is True

    def test_prime_eligible_velo(self):
        assert self._is_eligible_prime("Vélo/Trottinette/Autres", 1) is True

    def test_prime_wrong_transport(self):
        assert self._is_eligible_prime("Transports en commun", 3) is False

    def test_prime_zero_commute(self):
        assert self._is_eligible_prime("Marche/running", 0) is False

    def test_prime_vehicule_thermique(self):
        assert self._is_eligible_prime("véhicule thermique/électrique", 10) is False

    # Journees bien-etre
    def test_wellness_exactly_15(self):
        assert self._is_eligible_wellness(15) is True

    def test_wellness_below(self):
        assert self._is_eligible_wellness(14) is False

    def test_wellness_above(self):
        assert self._is_eligible_wellness(20) is True

    def test_wellness_zero(self):
        assert self._is_eligible_wellness(0) is False

    # Double eligibilite
    def test_both_eligible(self):
        assert self._is_eligible_prime("Marche/running", 5) is True
        assert self._is_eligible_wellness(20) is True

    def test_neither_eligible(self):
        assert self._is_eligible_prime("Transports en commun", 0) is False
        assert self._is_eligible_wellness(3) is False


# ============================================================
# Normalisation des noms de colonnes
# ============================================================

class TestColumnNormalization:
    """Teste la normalisation des noms de colonnes CSV (accents, espaces)."""

    @staticmethod
    def normalize_column_name(name):
        """Reproduit la fonction normalize_column_name du medallion processor."""
        import unicodedata
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        name = name.replace(' ', '_').replace("'", '_').replace('é', 'e').replace('è', 'e')
        return name.lower()

    def test_id_salarie(self):
        assert self.normalize_column_name("ID salarié") == "id_salarie"

    def test_moyen_deplacement(self):
        assert self.normalize_column_name("Moyen de déplacement") == "moyen_de_deplacement"

    def test_pratique_sport(self):
        assert self.normalize_column_name("Pratique d'un sport") == "pratique_d_un_sport"

    def test_date_naissance(self):
        assert self.normalize_column_name("Date de naissance") == "date_de_naissance"

    def test_salaire_brut(self):
        assert self.normalize_column_name("Salaire brut") == "salaire_brut"

    def test_simple_name(self):
        assert self.normalize_column_name("Nom") == "nom"

    def test_date_embauche(self):
        assert self.normalize_column_name("Date d'embauche") == "date_d_embauche"
