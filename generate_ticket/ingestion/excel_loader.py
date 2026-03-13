import pandas as pd
from config.configuration import Settings, settings as default_settings
from config.logger import logger

REQUIRED_RH_COLUMNS = {"ID salarié", "Moyen de déplacement"}
REQUIRED_SPORT_COLUMNS = {"ID salarié", "Pratique d'un sport"}


def load_employee_data(settings: Settings | None = None) -> pd.DataFrame:
    """Charge et fusionne les données RH et sportives depuis les fichiers Excel."""
    if settings is None:
        settings = default_settings

    rh_path = settings.xlsx_employees_full_path
    sport_path = settings.xlsx_sport_full_path

    if not rh_path.exists():
        raise FileNotFoundError(f"Fichier RH introuvable : {rh_path}")
    if not sport_path.exists():
        raise FileNotFoundError(f"Fichier sportif introuvable : {sport_path}")

    df_rh = pd.read_excel(rh_path)
    df_sport = pd.read_excel(sport_path)

    # Validation des colonnes attendues
    missing_rh = REQUIRED_RH_COLUMNS - set(df_rh.columns)
    if missing_rh:
        raise ValueError(f"Colonnes manquantes dans le fichier RH : {missing_rh}")

    missing_sport = REQUIRED_SPORT_COLUMNS - set(df_sport.columns)
    if missing_sport:
        raise ValueError(f"Colonnes manquantes dans le fichier sportif : {missing_sport}")

    df = pd.merge(df_rh, df_sport, on="ID salarié", how="left")

    if df.empty:
        logger.warning("La jointure RH/Sport a produit un DataFrame vide.")

    # Renommer les colonnes pour un usage interne propre
    df = df.rename(columns={
        "ID salarié": "employee_id",
        "Pratique d'un sport": "sport_practice",
        "Moyen de déplacement": "transport_mode",
    })

    return df
