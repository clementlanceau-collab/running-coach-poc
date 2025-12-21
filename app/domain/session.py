from dataclasses import dataclass
from datetime import date
from typing import Optional, List


@dataclass
class Session:
    external_id: int
    source: str
    has_streams: bool
    date: date

    sport_type: str
    session_type: str
    duration_min: int
    distance_km: float
    elevation_m: int
    intensity_level: str

    device_name: Optional[str] = None
    has_heartrate: Optional[bool] = None
    hr_source: Optional[str] = None

    data_confidence_gps: Optional[float] = None
    data_flags: Optional[List[str]] = None
