from kafka_consumer import consumer
from config.logger import logger
from datetime import datetime


def get_latest_activity() -> dict | None:
    """
    Consomme tous les messages disponibles dans Kafka
    et retourne uniquement le dernier reçu.
    Retourne None si aucun message n'est disponible.
    """
    latest = None

    for message in consumer:
        activity = message.value
        if activity is None:
            continue

        logger.info(f"MESSAGE REÇU : {activity}")

        start_timestamp_micro = activity.get("start_timestamp")
        if start_timestamp_micro:
            start_datetime = datetime.utcfromtimestamp(start_timestamp_micro / 1_000_000)
        else:
            start_datetime = None

        latest = {
            "activity_id": activity.get("id"),
            "employee_id": activity.get("employee_id"),
            "sport_type": activity.get("sport_type"),
            "distance": activity.get("distance"),
            "elapsed_time": activity.get("elapsed_time"),
            "start_timestamp": start_datetime,
            "details": activity.get("details"),
            "is_deleted": activity.get("__deleted") == "true",
        }

    return latest
