#!/bin/bash
set -e

echo "================================================"
echo " Pipeline de données sportives — OpenClassrooms"
echo "================================================"

# Infrastructure Docker
docker-compose up -d

# Attendre que postgres-init soit terminé
echo "En attente de l'initialisation de PostgreSQL..."
docker wait postgres-init
echo "PostgreSQL prêt !"

# Environnement Python
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# Activation venv — détection automatique OS
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi


pip install -r requirements.txt

# Génération des tickets
python3 -m generate_ticket.main

echo "================================================"
echo " Pipeline démarré avec succès !"
echo "================================================"