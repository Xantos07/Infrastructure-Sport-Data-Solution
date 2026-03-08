
# Infrastructure Sport Data Solution


## Prérequis

- [Docker](https://www.docker.com/) et [Docker Compose](https://docs.docker.com/compose/)
- Python 3.13.1+

## Installation

1. Clonez ce dépôt :
	```bash
	git clone <url-du-repo>
	cd Infrastructure-Sport-Data-Solution
	```
2. Installez les dépendances Python :
	```bash
	python -m venv venv
	venv\Scripts\activate  # Windows
	pip install -r requirements.txt
	```

## Lancement de l'infrastructure

Lancez tous les services (PostgreSQL, Redpanda, Debezium, pgAdmin) :

```bash
docker-compose up -d
```## Accès rapides

- **pgAdmin** : [http://localhost:8081](http://localhost:8081) (login: admin@example.com / admin)
- **Redpanda Console** : [http://localhost:8080](http://localhost:8080)

## Configuration

Adaptez les fichiers `database.ini` et `debezium.json` selon vos besoins.


## Debezium
Sert à capturer les changements dans la base de données PostgreSQL et à les publier dans Kafka (Redpanda).
```bash
Get-Content debezium.json | docker exec -i debezium-connector bash -c "curl -X POST -H 'Content-Type: application/json' http://localhost:8083/connectors -d @-"
```

### streaming avec Spark + Delta Lake

Copier le script et lancer le consumer Spark avec persistance Delta Lake :

```bash
docker cp .\spark_consumer.py spark-master:/opt/spark/work/spark_consumer.py
docker exec spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 /opt/spark/work/spark_consumer.py
```

Les données sont persistées dans Delta Lake au chemin `/opt/spark/delta/activities` (volume Docker `delta_data`).

### Vérifier les données Delta Lake

Pour lire les données persistées dans Delta Lake :

```bash
docker cp .\delta_reader.py spark-master:/opt/spark/work/delta_reader.py
docker exec spark-master /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark/work/delta_reader.py
```

Ce script affiche :
- Le nombre total d'activités persistées
- Le schéma des données
- Les activités par type de sport
- Les activités par employé
- L'historique des versions Delta Lake