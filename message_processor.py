from kafka_consumer import consumer
from logger import logger
from datetime import datetime

data = []

try:
    for message in consumer:
        topic_info = f"topic: {message.partition}|{message.offset})"
        activity = message.value
        is_deleted = activity.get("__deleted") == 'true'

        logger.info(f"MESSAGE BRUT REÇU : {activity}")
        
        if is_deleted:
            message_info = f"L'activité sportive pour l'employé {activity['id']} a été supprimée."
        else:
            message_info = f"Nouvelle activité sportive pour l'employé {activity['id']}: {activity['sport_type']} : {activity['distance']}m en {activity['elapsed_time']}s"

      
        # Convertir le timestamp Debezium en datetime Python
        start_timestamp_micro = activity.get('start_timestamp')
        logger.info(f"TIMESTAMP BRUT: {start_timestamp_micro} (type: {type(start_timestamp_micro)})")
        
        if start_timestamp_micro:
            # évite les décalages de timezone
            start_datetime = datetime.utcfromtimestamp(start_timestamp_micro / 1_000_000)
            logger.info(f"TIMESTAMP CONVERTI: {start_datetime}")
        else:
            start_datetime = None
        
        data.append({
            "activity_id": activity.get('id'),
            "employee_id": activity.get('employee_id'),
            "sport_type": activity.get('sport_type'),
            "distance": activity.get('distance'),
            "elapsed_time": activity.get('elapsed_time'),
            "start_timestamp": start_datetime, 
            "details": activity.get('details'),
            "is_deleted": is_deleted
        })
        logger.info(f"{topic_info}, {message_info}")
except Exception as e:
    logger.error(f"Error occurred while consuming messages: {e}")
finally:
    consumer.close()