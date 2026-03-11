<#
.SYNOPSIS
    Pipeline Medallion complet : Bronze → Silver → Gold → Parquet pour Power BI Desktop.

.DESCRIPTION
    Ce script :
    1. Prépare les données de référence (Excel → CSV) sur le host
    2. Copie les scripts dans le conteneur Spark
    3. Lance le processeur Medallion (Bronze → Silver → Gold)
    4. Les fichiers Parquet sont générés automatiquement dans powerbi_data/

.PARAMETER Mode
    single : traitement unique (défaut)
    watch  : traitement continu toutes les N secondes

.EXAMPLE
    .\start_powerbi_export.ps1
    .\start_powerbi_export.ps1 -Mode watch
    .\start_powerbi_export.ps1 -Mode watch -Interval 60
#>
param(
    [ValidateSet("single", "watch")]
    [string]$Mode = "single",
    [int]$Interval = 60
)

$ErrorActionPreference = "Stop"
$container = "spark-master"
$medallionScript = "spark_medallion_processor.py"
$localMedallion = Join-Path $PSScriptRoot $medallionScript
$remoteMedallion = "/opt/spark/work/$medallionScript"

Write-Host "=== Pipeline Medallion → Power BI ===" -ForegroundColor Cyan
Write-Host ""

# Vérifier que le conteneur Spark est en cours d'exécution
$running = docker ps --filter "name=$container" --format "{{.Names}}" 2>$null
if ($running -ne $container) {
    Write-Host "Le conteneur '$container' n'est pas en cours d'execution." -ForegroundColor Red
    Write-Host "Lancez d'abord : docker compose up -d" -ForegroundColor Yellow
    exit 1
}

# Étape 1 : Préparer les données de référence (Excel -> CSV)
Write-Host "[1/3] Preparation des donnees de reference (Excel -> CSV)..." -ForegroundColor Yellow
python (Join-Path $PSScriptRoot "prepare_reference_data.py")
if ($LASTEXITCODE -ne 0) {
    Write-Host "Erreur lors de la preparation des donnees de reference." -ForegroundColor Red
    exit 1
}

# Étape 2 : Copier le script Medallion dans le conteneur
Write-Host "[2/3] Copie du processeur Medallion dans le conteneur..." -ForegroundColor Yellow
docker cp $localMedallion "${container}:${remoteMedallion}"

# Étape 3 : Lancer le processeur Medallion
Write-Host "[3/3] Lancement du pipeline Medallion (mode: $Mode)..." -ForegroundColor Green
Write-Host ""

$sparkArgs = "--mode", $Mode
if ($Mode -eq "watch") {
    $sparkArgs += "--interval", $Interval
}

docker exec -it $container /opt/spark/bin/spark-submit `
    --packages "io.delta:delta-spark_2.12:3.2.0" `
    $remoteMedallion $sparkArgs

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Pipeline Medallion termine avec succes !" -ForegroundColor Green
    Write-Host ""
    Write-Host "Fichiers Parquet disponibles :" -ForegroundColor Cyan
    Write-Host "  - powerbi_data\employee_eligibility.parquet\  (Gold : eligibilite)" -ForegroundColor White
    Write-Host "  - powerbi_data\activities.parquet\             (Silver : activites)" -ForegroundColor White
    Write-Host ""
    Write-Host "Dans Power BI Desktop :" -ForegroundColor Yellow
    Write-Host "  1. Accueil > Obtenir les donnees > Parquet" -ForegroundColor White
    Write-Host "  2. Selectionnez employee_eligibility.parquet (ou activities.parquet)" -ForegroundColor White
    Write-Host "  3. Cliquez sur Charger" -ForegroundColor White
} else {
    Write-Host "Erreur lors du pipeline Medallion." -ForegroundColor Red
}
