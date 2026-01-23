
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
```

## Accès rapides

- **pgAdmin** : [http://localhost:8081](http://localhost:8081) (login: admin@example.com / admin)
- **Redpanda Console** : [http://localhost:8080](http://localhost:8080)

## Configuration

Adaptez les fichiers `database.ini` et `debezium.json` selon vos besoins.





