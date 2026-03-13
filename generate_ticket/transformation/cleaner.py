import pandas as pd
from generate_ticket.constants import TRANSPORT_MODES


def remove_non_eligible(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les employés non éligibles à la prime sportive.
    Non éligible = pas de sport extérieur ET pas de mode de transport sportif.
    """
    mask_non_eligible = (df["sport_practice"].isna()) & (~df["transport_mode"].isin(TRANSPORT_MODES))
    df_clean = df.drop(df[mask_non_eligible].index).copy()
    return df_clean


def split_by_eligibility(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Sépare le DataFrame en 3 groupes :
    - sport_only       : sport extérieur uniquement
    - transport_only   : déplacement sportif au travail uniquement
    - both             : sport extérieur + déplacement sportif
    """
    df_sport_only = df[
        df["sport_practice"].notna() & ~df["transport_mode"].isin(TRANSPORT_MODES)
    ]
    df_transport_only = df[
        df["sport_practice"].isna() & df["transport_mode"].isin(TRANSPORT_MODES)
    ]
    df_both = df[
        df["sport_practice"].notna() & df["transport_mode"].isin(TRANSPORT_MODES)
    ]

    return df_sport_only, df_transport_only, df_both
