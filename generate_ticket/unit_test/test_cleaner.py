import pandas as pd
import pytest

from generate_ticket.transformation.cleaner import remove_non_eligible, split_by_eligibility


@pytest.fixture
def sample_df():
    """DataFrame de test avec les 4 catégories d'employés."""
    return pd.DataFrame({
        "employee_id": [1, 2, 3, 4],
        "sport_practice": ["Running", None, "Natation", None],
        "transport_mode": ["véhicule thermique/électrique", "Marche/running", "Vélo/Trottinette/Autres", "Transports en commun"],
    })


class TestRemoveNonEligible:
    def test_removes_non_eligible(self, sample_df):
        result = remove_non_eligible(sample_df)
        # Employé 4 (pas de sport, transport non sportif) doit être retiré
        assert 4 not in result["employee_id"].values
        assert len(result) == 3

    def test_keeps_sport_only(self, sample_df):
        result = remove_non_eligible(sample_df)
        assert 1 in result["employee_id"].values

    def test_keeps_transport_only(self, sample_df):
        result = remove_non_eligible(sample_df)
        assert 2 in result["employee_id"].values

    def test_keeps_both(self, sample_df):
        result = remove_non_eligible(sample_df)
        assert 3 in result["employee_id"].values

    def test_empty_df(self):
        df = pd.DataFrame(columns=["employee_id", "sport_practice", "transport_mode"])
        result = remove_non_eligible(df)
        assert len(result) == 0

    def test_does_not_mutate_original(self, sample_df):
        original_len = len(sample_df)
        remove_non_eligible(sample_df)
        assert len(sample_df) == original_len


class TestSplitByEligibility:
    def test_split_counts(self, sample_df):
        df_clean = remove_non_eligible(sample_df)
        sport_only, transport_only, both = split_by_eligibility(df_clean)

        assert len(sport_only) == 1      # Employé 1
        assert len(transport_only) == 1  # Employé 2
        assert len(both) == 1            # Employé 3

    def test_sport_only_ids(self, sample_df):
        df_clean = remove_non_eligible(sample_df)
        sport_only, _, _ = split_by_eligibility(df_clean)
        assert sport_only["employee_id"].tolist() == [1]

    def test_transport_only_ids(self, sample_df):
        df_clean = remove_non_eligible(sample_df)
        _, transport_only, _ = split_by_eligibility(df_clean)
        assert transport_only["employee_id"].tolist() == [2]

    def test_both_ids(self, sample_df):
        df_clean = remove_non_eligible(sample_df)
        _, _, both = split_by_eligibility(df_clean)
        assert both["employee_id"].tolist() == [3]

    def test_no_overlap(self, sample_df):
        df_clean = remove_non_eligible(sample_df)
        sport_only, transport_only, both = split_by_eligibility(df_clean)
        all_ids = (
            set(sport_only["employee_id"])
            | set(transport_only["employee_id"])
            | set(both["employee_id"])
        )
        assert len(all_ids) == len(sport_only) + len(transport_only) + len(both)
