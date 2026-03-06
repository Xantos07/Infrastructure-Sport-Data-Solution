import requests
from configuration import Settings
import json
from message_processor import data
from logger import logger

settings = Settings()

logger.info("Final DataFrame:")
logger.info(data)


# juste 5 fois pour ne pas spammer le webhook Discord
for index, row in enumerate(data):
    if index >= 5:
        break

    distance = row['distance'] if row['distance'] is not None else 0
    embed = {
        ## mettre le bon nom de la personne par rapport à l'id comme dans la consigne 
        "title": f"Nouvelle activité sportive pour l'employé {row['employee_id']}",
        "description": f"Séance de {row['sport_type']} le {row['start_timestamp'].strftime('%d/%m/%Y à %H:%M')}\n{distance}m en {row['elapsed_time'] / 60:.1f} mins\n\nDétails: {row['details']}",
    }
    data = {
    "embeds": [embed]
    }

    response = requests.post(settings.discord_webhook_url, json=data)
    message = json.dumps({"embeds": [embed]})
    logger.info(f"Message envoyé à Discord: {message}")