FROM python:3.11-slim
 
WORKDIR /app
 
# Dépendances système minimales
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*
 
# Copie et install des dependances Python (requirements propres au consumer Discord)
COPY discord_consumer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Fichiers partagés depuis la racine
COPY config/ ./config/
COPY kafka_consumer.py .

# Copie des scripts consumer
COPY discord_consumer/message_processor.py .
COPY discord_consumer/notification_discord_consumer.py .

# Variables d'env attendues (injectées via docker-compose)
# BOOTSTRAP_SERVERS, DISCORD_WEBHOOK_URL, KAFKA_TOPIC

CMD ["python", "-u", "notification_discord_consumer.py"]