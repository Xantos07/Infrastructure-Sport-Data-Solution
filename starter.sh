Write-Host "================================================"
Write-Host " Pipeline de données sportives — OpenClassrooms"
Write-Host "================================================"

# Infrastructure Docker
docker-compose up -d

# Attendre que postgres-init soit terminé (lui-même attend que postgres soit healthy)
Write-Host "En attente de l'initialisation de PostgreSQL..."
docker-compose wait postgres-init

Write-Host "PostgreSQL prêt !"

# Environnement Python
if (-Not (Test-Path "venv")) {
    py -m venv venv
}

.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Génération des tickets
py -m generate_ticket.main

Write-Host "================================================"
Write-Host " Pipeline démarré avec succès !"
Write-Host "================================================"