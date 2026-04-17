import pytest
from generate_ticket.models.ticket import ActivityTicket
from datetime import datetime


class TestActivityTicket:
    def test_create_ticket(self):
        ticket = ActivityTicket(
            employee_id=1,
            start_time=datetime(2026, 3, 10, 8, 30),
            sport_type="Running",
            distance_meters=5000,
            elapsed_time_seconds=1800,
            details="Super séance !",
        )
        assert ticket.employee_id == 1
        assert ticket.sport_type == "Running"
        assert ticket.distance_meters == 5000

    def test_optional_distance(self):
        ticket = ActivityTicket(
            employee_id=2,
            start_time=datetime(2026, 3, 10, 18, 0),
            sport_type="Yoga",
            distance_meters=None,
            elapsed_time_seconds=3600,
            details="Relaxant.",
        )
        assert ticket.distance_meters is None
