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
    py -m venv venv
fi

source venv/Scripts/Activate
pip install -r requirements.txt

# Génération des tickets
py -m generate_ticket.main

echo "================================================"
echo " Pipeline démarré avec succès !"
echo "================================================"