import os
import time
import requests
from config.configuration import DiscordSettings
from config.logger import logger
from message_processor import get_latest_activity
from kafka_consumer import consumer

settings = DiscordSettings()

INTERVAL_SEC = int(os.environ.get("DISCORD_INTERVAL", "300"))  # 5 minutes par défaut


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

    response = requests.post(str(settings.discord_webhook_url), json={"embeds": [embed]})
    logger.info(f"Discord notifié (status {response.status_code}) pour l'employé {row['employee_id']}")


def main() -> None:
    logger.info(f"Consumer démarré — intervalle : {INTERVAL_SEC}s")
    try:
        while True:
            activity = get_latest_activity()
            if activity:
                send_activity(activity)
            else:
                logger.info("Aucun nouveau message Kafka.")

            logger.info(f"Prochaine notification dans {INTERVAL_SEC}s...")
            time.sleep(INTERVAL_SEC)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
