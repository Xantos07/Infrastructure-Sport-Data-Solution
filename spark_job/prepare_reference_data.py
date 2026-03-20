"""
Prépare les données de référence (Excel → CSV) pour Apache Spark.

Les fichiers Excel de Inputs/ ne sont pas lisibles nativement par Spark dans Docker.
Ce script les convertit en CSV pour être montés et lus par le processeur Medallion.

Usage :
  python prepare_reference_data.py
"""

import pandas as pd
from config.configuration import BaseAppSettings
import os

csv_rh = "/opt/spark/delta/inputs/employees.csv"
csv_sport = "/opt/spark/delta/inputs/sports.csv"

def main():
    settings = BaseAppSettings()

    # Crée le dossier cible si besoin, A CORRIGER
    os.makedirs(os.path.dirname(csv_rh), exist_ok=True)
    os.makedirs(os.path.dirname(csv_sport), exist_ok=True)

    # DonneesRH.xlsx → employees.csv
    df_rh = pd.read_excel(settings.xlsx_employees_full_path)
    df_rh.to_csv(csv_rh, index=False, encoding="utf-8")
    print(f"Exporté : {csv_rh} ({len(df_rh)} lignes)")
    print(f"  Colonnes : {list(df_rh.columns)}")

    # DonneesSportive.xlsx → sports.csv
    df_sport = pd.read_excel(settings.xlsx_sport_full_path)
    df_sport.to_csv(csv_sport, index=False, encoding="utf-8")
    print(f"Exporté : {csv_sport} ({len(df_sport)} lignes)")
    print(f"  Colonnes : {list(df_sport.columns)}")


if __name__ == "__main__":
    main()
