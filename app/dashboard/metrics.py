from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List
import math
import sqlite3

from .db import q


def fmt_hms(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    s = int(round(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m}:{sec:02d}"


def fmt_km(meters: Optional[float]) -> str:
    if meters is None:
        return "—"
    return f"{meters/1000.0:.3f} km"


def fmt_int(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return str(int(round(x)))


def fmt_float(x: Optional[float], nd: int = 1) -> str:
    if x is None:
        return "—"
    return f"{x:.{nd}f}"


def pace_s_per_km(elapsed_s: float, dist_m: float) -> Optional[float]:
    if dist_m <= 0:
        return None
    return (elapsed_s / dist_m) * 1000.0


def fmt_pace(pace: Optional[float]) -> str:
    if pace is None or pace <= 0:
        return "—"
    total = int(round(pace))
    mm = total // 60
    ss = total % 60
    return f"{mm}:{ss:02d}/km"


def safe_std(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return 0.0 if len(vals) == 1 else None
    m = sum(vals) / len(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (len(vals) - 1))


# -----------------------------
# Data access helpers (assumes existing tables: activities, stream_points)
# -----------------------------

def list_recent_runs(conn: sqlite3.Connection, limit: int = 20) -> List[sqlite3.Row]:
    return q(conn, """
        SELECT activity_id, start_date_local, name, sport_type
        FROM activities
        WHERE sport_type IN ('Run','Trail Run')
          AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT ?;
    """, (limit,))


def get_activity_meta(conn: sqlite3.Connection, activity_id: int) -> Optional[sqlite3.Row]:
    rows = q(conn, """
        SELECT activity_id, start_date_local, name, sport_type, device_name, has_heartrate
        FROM activities
        WHERE activity_id = ?;
    """, (activity_id,))
    return rows[0] if rows else None


def get_activity_context(conn: sqlite3.Connection, activity_id: int) -> Optional[sqlite3.Row]:
    rows = q(conn, """
        SELECT activity_id, terrain_type, surface_note, shoes
        FROM activity_context
        WHERE activity_id = ?;
    """, (activity_id,))
    return rows[0] if rows else None


def get_session_rpe(conn: sqlite3.Connection, activity_id: int) -> Optional[sqlite3.Row]:
    rows = q(conn, """
        SELECT activity_id, rpe, note
        FROM session_rpe
        WHERE activity_id = ?;
    """, (activity_id,))
    return rows[0] if rows else None


def get_session_intensity_declared(conn: sqlite3.Connection, activity_id: int) -> Dict[str, float]:
    rows = q(conn, """
        SELECT bucket, seconds
        FROM session_intensity
        WHERE activity_id = ?;
    """, (activity_id,))
    return {r["bucket"]: float(r["seconds"]) for r in rows}


def activity_totals_from_streams(conn: sqlite3.Connection, activity_id: int) -> Dict[str, Any]:
    """
    Totaux factuels depuis stream_points :
    - duration_s : max(idx)-min(idx)+1 approx si 1Hz, mais on ne doit pas l'inventer.
      On utilise elapsed_s si dispo dans activities, sinon on approx via COUNT.
    Hypothèse minimale: stream_points contient distance_m cumulée, altitude_m, heartrate_bpm.
    """
    # distance: max(distance_m) - min(distance_m)
    dist_rows = q(conn, """
        SELECT MIN(distance_m) AS d0, MAX(distance_m) AS d1
        FROM stream_points
        WHERE activity_id = ?;
    """, (activity_id,))
    d0 = dist_rows[0]["d0"] if dist_rows else None
    d1 = dist_rows[0]["d1"] if dist_rows else None
    dist_m = None
    if d0 is not None and d1 is not None:
        dist_m = float(d1) - float(d0)

    # duration: if stream is 1Hz, count ~ seconds. If not sure, we still return count_points.
    n_rows = q(conn, """
        SELECT COUNT(*) AS n
        FROM stream_points
        WHERE activity_id = ?;
    """, (activity_id,))
    n_points = int(n_rows[0]["n"]) if n_rows else 0

    # HR avg if present
    hr_rows = q(conn, """
        SELECT AVG(heartrate_bpm) AS hr_avg
        FROM stream_points
        WHERE activity_id = ?
          AND heartrate_bpm IS NOT NULL;
    """, (activity_id,))
    hr_avg = hr_rows[0]["hr_avg"] if hr_rows else None
    hr_avg = float(hr_avg) if hr_avg is not None else None

    # Elevation gain/loss (simple point-to-point positive/negative deltas)
    alt_rows = q(conn, """
        SELECT idx, altitude_m
        FROM stream_points
        WHERE activity_id = ?
          AND altitude_m IS NOT NULL
        ORDER BY idx ASC;
    """, (activity_id,))
    dplus = 0.0
    dminus = 0.0
    if len(alt_rows) >= 2:
        prev = float(alt_rows[0]["altitude_m"])
        for r in alt_rows[1:]:
            cur = float(r["altitude_m"])
            diff = cur - prev
            if diff > 0:
                dplus += diff
            elif diff < 0:
                dminus += -diff
            prev = cur

    return {
        "dist_m": dist_m,
        "n_points": n_points,
        "hr_avg": hr_avg,
        "dplus_m": dplus if dplus > 0 else 0.0,
        "dminus_m": dminus if dminus > 0 else 0.0
    }


# -----------------------------
# Dashboard aggregates (time windows)
# -----------------------------

def activities_in_range(conn: sqlite3.Connection, date_from: str, date_to: str) -> List[sqlite3.Row]:
    """
    date_from/date_to expected in ISO local format comparable as strings.
    """
    return q(conn, """
        SELECT activity_id, start_date_local, sport_type, name
        FROM activities
        WHERE start_date_local >= ? AND start_date_local < ?
        ORDER BY start_date_local ASC;
    """, (date_from, date_to))


def group_counts_by_sport(conn: sqlite3.Connection, date_from: str, date_to: str) -> Dict[str, int]:
    rows = q(conn, """
        SELECT sport_type, COUNT(*) AS n
        FROM activities
        WHERE start_date_local >= ? AND start_date_local < ?
        GROUP BY sport_type;
    """, (date_from, date_to))
    return {r["sport_type"]: int(r["n"]) for r in rows}


def run_days_and_off_days(conn: sqlite3.Connection, date_from: str, date_to: str) -> Dict[str, Any]:
    """
    'jours courus' = nb de dates distinctes avec Run/Trail Run.
    'jours off réels' = nb de jours dans l'intervalle sans Run/Trail Run.
    """
    run_days = q(conn, """
        SELECT substr(start_date_local, 1, 10) AS day, COUNT(*) AS n
        FROM activities
        WHERE sport_type IN ('Run','Trail Run')
          AND start_date_local >= ? AND start_date_local < ?
        GROUP BY substr(start_date_local, 1, 10);
    """, (date_from, date_to))
    days_with_run = {r["day"] for r in run_days}

    # count days in interval (inclusive start, exclusive end)
    # date strings are ISO; we compute days count in caller if needed.
    return {
        "days_with_run": sorted(days_with_run),
        "n_days_with_run": len(days_with_run)
    }
