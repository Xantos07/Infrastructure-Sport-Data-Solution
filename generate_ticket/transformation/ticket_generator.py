import numpy as np
import pandas as pd
from config.logger import logger

from generate_ticket.constants import (
    COMMENTAIRES_SPORT,
    COMMENTAIRES_TRANSPORT,
    SPORT_TYPES_WITH_DISTANCE,
    TRANSPORT_MODES,
)
from generate_ticket.models.ticket import ActivityTicket

# Générateur aléatoire reproductible (seed configurable)
#_rng = np.random.default_rng(seed=42)
_rng = np.random.default_rng()

def set_seed(seed: int) -> None:
    """Réinitialise le générateur aléatoire avec une seed donnée."""
    global _rng
    _rng = np.random.default_rng(seed=seed)


def generate_tickets(
    df_sport_only: pd.DataFrame,
    df_transport_only: pd.DataFrame,
    df_both: pd.DataFrame,
    total: int = 500,
) -> list[ActivityTicket]:
    """Génère exactement *total* tickets, répartis proportionnellement à la taille des groupes."""
    groups = [
        (df_sport_only, df_sport_only["employee_id"].tolist(), "sport"),
        (df_transport_only, df_transport_only["employee_id"].tolist(), "transport"),
        (df_both, df_both["employee_id"].tolist(), "both"),
    ]

    # Filtrer les groupes vides
    active_groups = [(df, ids, mode) for df, ids, mode in groups if ids]
    if not active_groups:
        logger.warning("Aucun groupe éligible — 0 ticket généré.")
        return []

    # Distribution proportionnelle à la taille de chaque groupe
    sizes = np.array([len(ids) for _, ids, _ in active_groups], dtype=float)
    weights = sizes / sizes.sum()

    all_tickets: list[ActivityTicket] = []

    for _ in range(total):
        idx = _rng.choice(len(active_groups), p=weights)
        group_df, group_ids, mode = active_groups[idx]
        all_tickets.extend(_generate_for_employee(group_df, group_ids, mode))

    logger.info(f"{len(all_tickets)} tickets générés au total.")
    return all_tickets


# ---------------------------------------------------------------------------
#  Fonctions internes de génération
# ---------------------------------------------------------------------------

def _generate_for_employee(
    employee_df: pd.DataFrame,
    employee_ids: list[int],
    mode: str,
) -> list[ActivityTicket]:
    """
    Génère un ticket pour un employé choisi aléatoirement dans le groupe.

    Args:
        mode: "sport" | "transport" | "both"
              - "both" choisit aléatoirement entre sport ou transport (pas les deux)
    """
    selected_id = _rng.choice(employee_ids)
    employee = employee_df[employee_df["employee_id"] == selected_id].iloc[0]

    tickets: list[ActivityTicket] = []

    # Pour le groupe "both", on choisit aléatoirement entre sport ou transport
    if mode == "both":
        mode = "sport" if _rng.random() < 0.5 else "transport"

    # Ticket Commute to work
    if mode == "transport" and employee["transport_mode"] in TRANSPORT_MODES:
        distance = int(_rng.integers(1000, 10000))
        elapsed = int(_rng.integers(15, 60)) * 60  # en secondes
        tickets.append(ActivityTicket(
            employee_id=int(selected_id),
            start_time=_random_commute_datetime(),
            sport_type=f"Commute to work - {employee['transport_mode']}",
            distance_meters=distance,
            elapsed_time_seconds=elapsed,
            details=str(_rng.choice(COMMENTAIRES_TRANSPORT)),
        ))

    # Ticket activité sportive extérieure
    if mode == "sport" and pd.notna(employee["sport_practice"]):
        activity_type = employee["sport_practice"]
        distance = int(_rng.integers(1000, 20000)) if activity_type in SPORT_TYPES_WITH_DISTANCE else None
        elapsed = int(_rng.integers(30, 120)) * 60  # en secondes
        tickets.append(ActivityTicket(
            employee_id=int(selected_id),
            start_time=_random_sport_datetime(),
            sport_type=str(activity_type),
            distance_meters=distance,
            elapsed_time_seconds=elapsed,
            details=str(_rng.choice(COMMENTAIRES_SPORT)),
        ))

    return tickets


# ---------------------------------------------------------------------------
#  Générateurs de dates
# ---------------------------------------------------------------------------

# Jours de semaine 2026 pré-calculés pour éviter la boucle while True
_YEAR_START = pd.Timestamp("2026-01-01")
_ALL_DAYS_2026 = pd.date_range("2026-01-01", "2026-12-31", freq="D")
_WEEKDAYS_2026 = _ALL_DAYS_2026[_ALL_DAYS_2026.weekday <= 4]


def _random_commute_datetime() -> pd.Timestamp:
    """Date aléatoire 2026, jour de semaine, entre 7h et 10h."""
    date = _rng.choice(_WEEKDAYS_2026)
    hour = int(_rng.integers(7, 11))
    minute = int(_rng.integers(0, 60))
    return pd.Timestamp(date).replace(hour=hour, minute=minute, second=0)


def _random_sport_datetime() -> pd.Timestamp:
    """Date aléatoire 2026 : 5h-22h le WE, 17h-22h en semaine."""
    date = _rng.choice(_ALL_DAYS_2026)
    date = pd.Timestamp(date)

    if date.weekday() > 4:  # Weekend
        hour = int(_rng.integers(5, 23))
    else:
        hour = int(_rng.integers(17, 23))

    minute = int(_rng.integers(0, 60))
    return date.replace(hour=hour, minute=minute, second=0)
