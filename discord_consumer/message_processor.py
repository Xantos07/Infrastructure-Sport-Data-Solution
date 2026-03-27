from kafka_consumer import consumer
from config.logger import get_logger
from datetime import datetime, timezone

logger = get_logger("discord_consumer.processor")


def _parse_activity(activity: dict) -> dict:
    """Transforme un message Kafka brut en dict d'activite."""
    start_timestamp_micro = activity.get("start_timestamp")
    if start_timestamp_micro:
        start_datetime = datetime.fromtimestamp(
            start_timestamp_micro / 1_000_000, tz=timezone.utc
        )
    else:
        start_datetime = None

    return {
        "activity_id": activity.get("id"),
        "employee_id": activity.get("employee_id"),
        "sport_type": activity.get("sport_type"),
        "distance": activity.get("distance"),
        "elapsed_time": activity.get("elapsed_time"),
        "start_timestamp": start_datetime,
        "details": activity.get("details"),
        "is_deleted": activity.get("__deleted") == "true",
    }


def get_all_activities() -> list[dict]:
    """
    Consomme tous les messages disponibles dans Kafka
    et retourne la liste de toutes les activites recues.
    Retourne une liste vide si aucun message n'est disponible.
    """
    activities = []

    for message in consumer:
        activity = message.value
        if activity is None:
            continue

        logger.info(f"MESSAGE REÇU : {activity}")
        activities.append(_parse_activity(activity))

    return activities


def get_latest_activity() -> dict | None:
    """
    Consomme tous les messages disponibles dans Kafka
    et retourne uniquement le dernier reçu.
    Retourne None si aucun message n'est disponible.

    Note : les messages intermediaires sont ignores.
    Utiliser get_all_activities() pour traiter chaque message.
    """
    activities = get_all_activities()
    return activities[-1] if activities else None
