import pandas as pd
import pytest

from generate_ticket.transformation.ticket_generator import (
    generate_tickets,
    set_seed,
    _random_commute_datetime,
    _random_sport_datetime,
)
from generate_ticket.models.ticket import ActivityTicket


@pytest.fixture(autouse=True)
def reset_seed():
    """Réinitialise la seed avant chaque test pour la reproductibilité."""
    set_seed(42)

@pytest.fixture
def groups():
    """3 groupes de test."""
    df_sport = pd.DataFrame({
        "employee_id": [1, 2],
        "sport_practice": ["Running", "Natation"],
        "transport_mode": ["véhicule thermique/électrique", "Transports en commun"],
    })
    df_transport = pd.DataFrame({
        "employee_id": [3],
        "sport_practice": [None],
        "transport_mode": ["Marche/running"],
    })
    df_both = pd.DataFrame({
        "employee_id": [4],
        "sport_practice": ["Triathlon"],
        "transport_mode": ["Vélo/Trottinette/Autres"],
    })
    return df_sport, df_transport, df_both


class TestGenerateTickets:
    def test_returns_list_of_activity_tickets(self, groups):
        tickets = generate_tickets(*groups, total=10)
        assert all(isinstance(t, ActivityTicket) for t in tickets)

    def test_generates_expected_count(self, groups):
        tickets = generate_tickets(*groups, total=50)
        # Chaque itération produit exactement 1 ticket → total attendu = 50
        assert len(tickets) == 50

    def test_reproducible_with_same_seed(self, groups):
        set_seed(123)
        tickets_a = generate_tickets(*groups, total=20)
        set_seed(123)
        tickets_b = generate_tickets(*groups, total=20)
        assert len(tickets_a) == len(tickets_b)
        for a, b in zip(tickets_a, tickets_b):
            assert a.employee_id == b.employee_id
            assert a.sport_type == b.sport_type

    def test_empty_groups(self):
        empty = pd.DataFrame(columns=["employee_id", "sport_practice", "transport_mode"])
        tickets = generate_tickets(empty, empty, empty, total=10)
        assert tickets == []

    def test_one_empty_group(self, groups):
        df_sport, _, df_both = groups
        empty = pd.DataFrame(columns=["employee_id", "sport_practice", "transport_mode"])
        tickets = generate_tickets(df_sport, empty, df_both, total=20)
        assert len(tickets) > 0
        # Aucun ticket ne doit venir du groupe transport (vide)
        for t in tickets:
            assert t.employee_id != 3


class TestDateGenerators:
    def test_commute_is_weekday(self):
        for _ in range(100):
            dt = _random_commute_datetime()
            assert dt.weekday() <= 4, f"{dt} n'est pas un jour de semaine"

    def test_commute_hour_range(self):
        for _ in range(100):
            dt = _random_commute_datetime()
            assert 7 <= dt.hour <= 10

    def test_sport_hour_weekday(self):
        for _ in range(200):
            dt = _random_sport_datetime()
            if dt.weekday() <= 4:
                assert 17 <= dt.hour <= 22
            else:
                assert 5 <= dt.hour <= 22

    def test_year_is_2026(self):
        for _ in range(50):
            assert _random_commute_datetime().year == 2026
            assert _random_sport_datetime().year == 2026
