import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.configuration import DiscordSettings
from config.logger import logger
from message_processor import get_latest_activity
from kafka_consumer import consumer

settings = DiscordSettings()

INTERVAL_SEC = int(os.environ.get("DISCORD_INTERVAL", "300"))  # 5 minutes par défaut
HTTP_TIMEOUT_SEC = int(os.environ.get("DISCORD_HTTP_TIMEOUT", "10"))


def _build_http_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP_SESSION = _build_http_session()


def send_activity(row: dict) -> None:
    distance = row["distance"] or 0
    ts = row["start_timestamp"].strftime("%d/%m/%Y à %H:%M") if row["start_timestamp"] else "date inconnue"

    if row["is_deleted"]:
        embed = {
            "title": f"Activité supprimée — Employé {row['employee_id']}",
            "color": 15158332,
        }
    else:
        embed = {
            "title": f"Dernière activité sportive — Employé {row['employee_id']}",
            "description": (
                f"**Sport :** {row['sport_type']}\n"
                f"**Date :** {ts}\n"
                f"**Distance :** {distance} m\n"
                f"**Durée :** {(row['elapsed_time'] or 0) / 60:.1f} min\n"
                f"**Détails :** {row['details']}"
            ),
            "color": 3066993,
        }

    response = HTTP_SESSION.post(
        str(settings.discord_webhook_url),
        json={"embeds": [embed]},
        timeout=HTTP_TIMEOUT_SEC,
    )
    response.raise_for_status()
    logger.info(f"Discord notifié (status {response.status_code}) pour l'employé {row['employee_id']}")


def main() -> None:
    if settings.discord_webhook_url is None:
        raise ValueError("DISCORD_WEBHOOK_URL est obligatoire pour notifier Discord.")

    logger.info(f"Consumer démarré — intervalle : {INTERVAL_SEC}s")
    try:
        while True:
            activity = get_latest_activity()
            if activity:
                try:
                    send_activity(activity)
                    consumer.commit()
                except requests.RequestException as e:
                    logger.error(f"Échec notification Discord (pas de commit offset): {e}")
            else:
                logger.info("Aucun nouveau message Kafka.")

            logger.info(f"Prochaine notification dans {INTERVAL_SEC}s...")
            time.sleep(INTERVAL_SEC)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
