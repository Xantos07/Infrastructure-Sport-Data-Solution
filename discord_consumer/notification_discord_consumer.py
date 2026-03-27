import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.configuration import DiscordSettings
from config.logger import get_logger

logger = get_logger("discord_consumer.notifier")
from message_processor import get_all_activities
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


def send_activity(row: dict, total_in_batch: int = 1) -> None:
    distance = row["distance"] or 0
    ts = row["start_timestamp"].strftime("%d/%m/%Y à %H:%M") if row["start_timestamp"] else "date inconnue"
    batch_note = f"\n*({total_in_batch} activité(s) dans ce lot)*" if total_in_batch > 1 else ""

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
                f"{batch_note}"
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
            activities = get_all_activities()
            if activities:
                # On commite tous les offsets (aucun message perdu)
                consumer.commit()
                # On envoie uniquement la dernière activité + un résumé du lot
                latest = activities[-1]
                try:
                    send_activity(latest, total_in_batch=len(activities))
                    logger.info(f"Lot de {len(activities)} activité(s) consommé — dernière notifiée sur Discord.")
                except requests.RequestException as e:
                    logger.error(f"Échec notification Discord : {e}")
            else:
                logger.info("Aucun nouveau message Kafka.")

            logger.info(f"Prochaine notification dans {INTERVAL_SEC}s...")
            time.sleep(INTERVAL_SEC)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
