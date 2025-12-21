from dataclasses import dataclass
from typing import Optional


@dataclass
class Lap:
    # --- Lien vers la séance ---
    session_id: str              # identifiant interne de la Session
    lap_index: int               # ordre dans la séance (0, 1, 2...)

    # --- Données temporelles ---
    duration_sec: int = 0
    distance_m: float = 0.0

    # --- Performance ---
    avg_pace_sec_per_km: Optional[float] = None
    avg_hr: Optional[int] = None
    avg_cadence: Optional[int] = None

    # --- Contexte ---
    lap_type: str = "unknown"    # work / recovery / warmup / race
