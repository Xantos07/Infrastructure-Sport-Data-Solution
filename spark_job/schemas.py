# spark_job/schemas.py
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, DoubleType
)

# ── Schéma Debezium → Bronze ──────────────────────────────
# Correspond au payload CDC de la table PostgreSQL 'activities'
ACTIVITY_DEBEZIUM_SCHEMA = StructType([
    StructField("id",              IntegerType(), True),
    StructField("employee_id",     IntegerType(), True),
    StructField("start_timestamp", LongType(),    True),  # microsecondes
    StructField("sport_type",      StringType(),  True),
    StructField("distance",        IntegerType(), True),
    StructField("elapsed_time",    IntegerType(), True),
    StructField("details",         StringType(),  True),
    StructField("__deleted",       StringType(),  True),
])

# ── Schéma CSV Employés ───────────────────────────────────
# Correspond au fichier employees.csv (DonneesRH.xlsx exporté)
EMPLOYEES_CSV_SCHEMA = StructType([
    StructField("ID salarié",            IntegerType(), True),
    StructField("Nom",                   StringType(),  True),
    StructField("Prénom",                StringType(),  True),
    StructField("Date de naissance",     StringType(),  True),
    StructField("BU",                    StringType(),  True),
    StructField("Date d'embauche",       StringType(),  True),
    StructField("Salaire brut",          IntegerType(),  True),
    StructField("Type de contrat",       StringType(),  True),
    StructField("Nombre de jours de CP", IntegerType(), True),
    StructField("Adresse du domicile",   StringType(),  True),
    StructField("Moyen de déplacement",  StringType(),  True),
])

# ── Schéma CSV Pratiques Sportives ───────────────────────────────────
#ID salarié,Pratique d'un sport
SPORTS_CSV_SCHEMA = StructType([
    StructField("ID salarié", IntegerType(), True),
    StructField("Pratique d'un sport", StringType(), True),
])
