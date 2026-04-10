import pandas as pd
from generate_ticket.constants import TRANSPORT_MODES
from config.logger import logger


def print_statistics(df: pd.DataFrame) -> None:
    """Affiche les statistiques d'éligibilité à la prime sportive."""
    total = len(df)
    if total == 0:
        logger.info("Aucun employé dans le DataFrame.")
        return

    transport_no_sport = len(df[df["sport_practice"].isna() & df["transport_mode"].isin(TRANSPORT_MODES)])
    sport_and_transport = len(df[df["sport_practice"].notna() & df["transport_mode"].isin(TRANSPORT_MODES)])
    sport_no_transport = len(df[df["sport_practice"].notna() & ~df["transport_mode"].isin(TRANSPORT_MODES)])
    non_eligible = len(df[df["sport_practice"].isna() & ~df["transport_mode"].isin(TRANSPORT_MODES)])

    sep = "-" * 88
    logger.info("=" * 30 + " Statistiques des employés " + "=" * 30)
    logger.info(sep)
    logger.info(f"Déplacement sportif sans sport extérieur : {transport_no_sport / total * 100:.2f}%")
    logger.info(sep)
    logger.info(f"Sport extérieur + déplacement sportif    : {sport_and_transport / total * 100:.2f}%")
    logger.info(sep)
    logger.info(f"Sport extérieur sans déplacement sportif  : {sport_no_transport / total * 100:.2f}%")
    logger.info(sep)
    logger.info(f"Non éligibles à la prime sportive         : {non_eligible / total * 100:.2f}%")
    logger.info(sep)
