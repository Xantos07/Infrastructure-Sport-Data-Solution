"""Tests unitaires pour le consumer Discord (send_activity, build_http_session)."""

from unittest.mock import patch, MagicMock
from datetime import datetime
import pytest
import requests


class TestBuildHttpSession:
    """Tests pour _build_http_session."""

    def test_returns_session(self):
        from discord_consumer.notification_discord_consumer import _build_http_session
        session = _build_http_session()
        assert isinstance(session, requests.Session)

    def test_has_retry_adapter(self):
        from discord_consumer.notification_discord_consumer import _build_http_session
        session = _build_http_session()
        assert "https://" in session.adapters
        assert "http://" in session.adapters


class TestSendActivity:
    """Tests pour send_activity."""

    @patch("discord_consumer.notification_discord_consumer.HTTP_SESSION")
    @patch("discord_consumer.notification_discord_consumer.settings")
    def test_sends_normal_activity(self, mock_settings, mock_session):
        mock_settings.discord_webhook_url = "https://discord.com/api/webhooks/test"
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_session.post.return_value = mock_response

        from discord_consumer.notification_discord_consumer import send_activity
        activity = {
            "employee_id": 42,
            "sport_type": "Running",
            "distance": 5000,
            "elapsed_time": 1800,
            "start_timestamp": datetime(2026, 3, 15, 8, 30),
            "details": "Super course",
            "is_deleted": False,
        }
        send_activity(activity)

        mock_session.post.assert_called_once()
        call_args = mock_session.post.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        embed = payload["embeds"][0]
        assert "42" in embed["title"]
        assert embed["color"] == 3066993  # vert

    @patch("discord_consumer.notification_discord_consumer.HTTP_SESSION")
    @patch("discord_consumer.notification_discord_consumer.settings")
    def test_sends_deleted_activity(self, mock_settings, mock_session):
        mock_settings.discord_webhook_url = "https://discord.com/api/webhooks/test"
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_session.post.return_value = mock_response

        from discord_consumer.notification_discord_consumer import send_activity
        activity = {
            "employee_id": 42,
            "sport_type": "Running",
            "distance": None,
            "elapsed_time": None,
            "start_timestamp": None,
            "details": None,
            "is_deleted": True,
        }
        send_activity(activity)

        call_args = mock_session.post.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        embed = payload["embeds"][0]
        assert "supprimée" in embed["title"].lower()
        assert embed["color"] == 15158332  # rouge

    @patch("discord_consumer.notification_discord_consumer.HTTP_SESSION")
    @patch("discord_consumer.notification_discord_consumer.settings")
    def test_handles_none_distance(self, mock_settings, mock_session):
        mock_settings.discord_webhook_url = "https://discord.com/api/webhooks/test"
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_session.post.return_value = mock_response

        from discord_consumer.notification_discord_consumer import send_activity
        activity = {
            "employee_id": 10,
            "sport_type": "Natation",
            "distance": None,
            "elapsed_time": 3600,
            "start_timestamp": datetime(2026, 1, 1, 10, 0),
            "details": "Piscine",
            "is_deleted": False,
        }
        send_activity(activity)
        mock_session.post.assert_called_once()

    @patch("discord_consumer.notification_discord_consumer.HTTP_SESSION")
    @patch("discord_consumer.notification_discord_consumer.settings")
    def test_handles_none_timestamp(self, mock_settings, mock_session):
        mock_settings.discord_webhook_url = "https://discord.com/api/webhooks/test"
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_session.post.return_value = mock_response

        from discord_consumer.notification_discord_consumer import send_activity
        activity = {
            "employee_id": 10,
            "sport_type": "Running",
            "distance": 5000,
            "elapsed_time": 1800,
            "start_timestamp": None,
            "details": "Test",
            "is_deleted": False,
        }
        send_activity(activity)

        call_args = mock_session.post.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        embed = payload["embeds"][0]
        assert "date inconnue" in embed["description"]

    @patch("discord_consumer.notification_discord_consumer.HTTP_SESSION")
    @patch("discord_consumer.notification_discord_consumer.settings")
    def test_raises_on_http_error(self, mock_settings, mock_session):
        mock_settings.discord_webhook_url = "https://discord.com/api/webhooks/test"
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        mock_session.post.return_value = mock_response

        from discord_consumer.notification_discord_consumer import send_activity
        activity = {
            "employee_id": 10,
            "sport_type": "Running",
            "distance": 5000,
            "elapsed_time": 1800,
            "start_timestamp": datetime(2026, 1, 1),
            "details": "Test",
            "is_deleted": False,
        }
        with pytest.raises(requests.HTTPError):
            send_activity(activity)

    @patch("discord_consumer.notification_discord_consumer.HTTP_SESSION")
    @patch("discord_consumer.notification_discord_consumer.settings")
    def test_batch_note_in_description(self, mock_settings, mock_session):
        """Quand un lot de plusieurs messages arrive, la description mentionne le total."""
        mock_settings.discord_webhook_url = "https://discord.com/api/webhooks/test"
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_session.post.return_value = mock_response

        from discord_consumer.notification_discord_consumer import send_activity
        activity = {
            "employee_id": 10,
            "sport_type": "Running",
            "distance": 5000,
            "elapsed_time": 1800,
            "start_timestamp": datetime(2026, 1, 1),
            "details": "Test",
            "is_deleted": False,
        }
        send_activity(activity, total_in_batch=42)

        call_args = mock_session.post.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        embed = payload["embeds"][0]
        assert "42" in embed["description"]

    @patch("discord_consumer.notification_discord_consumer.HTTP_SESSION")
    @patch("discord_consumer.notification_discord_consumer.settings")
    def test_single_message_no_batch_note(self, mock_settings, mock_session):
        """Quand total_in_batch=1, pas de note de lot dans la description."""
        mock_settings.discord_webhook_url = "https://discord.com/api/webhooks/test"
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_session.post.return_value = mock_response

        from discord_consumer.notification_discord_consumer import send_activity
        activity = {
            "employee_id": 10,
            "sport_type": "Running",
            "distance": 5000,
            "elapsed_time": 1800,
            "start_timestamp": datetime(2026, 1, 1),
            "details": "Test",
            "is_deleted": False,
        }
        send_activity(activity, total_in_batch=1)

        call_args = mock_session.post.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        embed = payload["embeds"][0]
        assert "lot" not in embed["description"]
