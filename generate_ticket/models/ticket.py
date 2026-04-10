from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ActivityTicket:
    """Modèle représentant un ticket d'activité sportive."""
    employee_id: int
    start_time: datetime
    sport_type: str
    distance_meters: Optional[int]
    elapsed_time_seconds: int
    details: str
