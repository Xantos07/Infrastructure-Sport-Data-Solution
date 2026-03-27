"""Tests unitaires pour le message_processor du consumer Discord."""

from datetime import datetime
from unittest.mock import MagicMock
import pytest


def _make_kafka_message(value):
    """Cree un faux message Kafka."""
    msg = MagicMock()
    msg.value = value
    return msg


class TestGetLatestActivity:
    """Tests pour get_latest_activity()."""

    def _call_with_messages(self, messages):
        """Helper : injecte les messages dans le consumer mocke et appelle get_latest_activity."""
        import sys
        mock_consumer_module = sys.modules["kafka_consumer"]
        mock_consumer_module.consumer.__iter__ = MagicMock(return_value=iter(messages))

        # Re-import pour prendre le mock
        from discord_consumer.message_processor import get_latest_activity
        return get_latest_activity()

    def test_returns_none_when_no_messages(self):
        result = self._call_with_messages([])
        assert result is None

    def test_returns_latest_when_multiple_messages(self):
        messages = [
            _make_kafka_message({"id": 1, "employee_id": 10, "sport_type": "Running",
                                 "distance": 5000, "elapsed_time": 1800,
                                 "start_timestamp": 1700000000_000000,
                                 "details": "Premier", "__deleted": None}),
            _make_kafka_message({"id": 2, "employee_id": 20, "sport_type": "Natation",
                                 "distance": 1000, "elapsed_time": 3600,
                                 "start_timestamp": 1700001000_000000,
                                 "details": "Dernier", "__deleted": None}),
        ]
        result = self._call_with_messages(messages)
        assert result is not None
        assert result["employee_id"] == 20
        assert result["sport_type"] == "Natation"
        assert result["details"] == "Dernier"

    def test_skips_none_values(self):
        messages = [
            _make_kafka_message(None),
            _make_kafka_message({"id": 1, "employee_id": 10, "sport_type": "Running",
                                 "distance": 5000, "elapsed_time": 1800,
                                 "start_timestamp": 1700000000_000000,
                                 "details": "Valide", "__deleted": None}),
        ]
        result = self._call_with_messages(messages)
        assert result is not None
        assert result["employee_id"] == 10

    def test_handles_deleted_activity(self):
        messages = [
            _make_kafka_message({"id": 1, "employee_id": 10, "sport_type": "Running",
                                 "distance": None, "elapsed_time": 1800,
                                 "start_timestamp": 1700000000_000000,
                                 "details": None, "__deleted": "true"}),
        ]
        result = self._call_with_messages(messages)
        assert result is not None
        assert result["is_deleted"] is True

    def test_handles_missing_timestamp(self):
        messages = [
            _make_kafka_message({"id": 1, "employee_id": 10, "sport_type": "Running",
                                 "distance": 5000, "elapsed_time": 1800,
                                 "start_timestamp": None,
                                 "details": "Pas de date", "__deleted": None}),
        ]
        result = self._call_with_messages(messages)
        assert result is not None
        assert result["start_timestamp"] is None

    def test_timestamp_conversion(self):
        ts_micro = 1700000000_000000
        messages = [
            _make_kafka_message({"id": 1, "employee_id": 10, "sport_type": "Running",
                                 "distance": 5000, "elapsed_time": 1800,
                                 "start_timestamp": ts_micro,
                                 "details": "Test", "__deleted": None}),
        ]
        result = self._call_with_messages(messages)
        assert isinstance(result["start_timestamp"], datetime)


class TestGetLatestActivityOutput:
    """Tests sur la structure de sortie."""

    def _call_with_messages(self, messages):
        import sys
        mock_consumer_module = sys.modules["kafka_consumer"]
        mock_consumer_module.consumer.__iter__ = MagicMock(return_value=iter(messages))
        from discord_consumer.message_processor import get_latest_activity
        return get_latest_activity()

    def test_output_has_all_keys(self):
        messages = [
            _make_kafka_message({"id": 1, "employee_id": 10, "sport_type": "Running",
                                 "distance": 5000, "elapsed_time": 1800,
                                 "start_timestamp": 1700000000_000000,
                                 "details": "Test", "__deleted": None}),
        ]
        result = self._call_with_messages(messages)
        expected_keys = {"activity_id", "employee_id", "sport_type", "distance",
                         "elapsed_time", "start_timestamp", "details", "is_deleted"}
        assert set(result.keys()) == expected_keys

    def test_not_deleted_is_false(self):
        messages = [
            _make_kafka_message({"id": 1, "employee_id": 10, "sport_type": "Running",
                                 "distance": 5000, "elapsed_time": 1800,
                                 "start_timestamp": 1700000000_000000,
                                 "details": "Test", "__deleted": None}),
        ]
        result = self._call_with_messages(messages)
        assert result["is_deleted"] is False
