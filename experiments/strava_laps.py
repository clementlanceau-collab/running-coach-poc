from dotenv import load_dotenv
load_dotenv()

import os
import time
import sqlite3
import requests
from typing import Optional, Dict, Any, List, Tuple

STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_API_BASE = "https://www.strava.com/api/v3"


# -----------------------
# OAuth + HTTP (identique à ton adapter)
# -----------------------
def refresh_access_token() -> str:
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")

    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Variables manquantes: STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN")

    body = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    r = requests.post(STRAVA_TOKEN_URL, data=body, timeout=20)
    data = r.json()
    if r.status_code != 200:
        raise RuntimeError(f"Erreur refresh token Strava: {data}")

    new_access = data.get("access_token")
    new_refresh = data.get("refresh_token")
    if not new_access:
        raise RuntimeError(f"Refresh sans access_token: {data}")

    os.environ["STRAVA_ACCESS_TOKEN"] = new_access
    if new_refresh:
        os.environ["STRAVA_REFRESH_TOKEN"] = new_refresh

    return new_access


def _get_access_token_or_refresh() -> str:
    access = os.getenv("STRAVA_ACCESS_TOKEN")
    if access:
        return access
    return refresh_access_token()


def _strava_get(url: str, params: Optional[dict] = None) -> requests.Response:
    token = _get_access_token_or_refresh()
    headers = {"Authorization": f"Bearer {token}"}

    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
    except requests.exceptions.RequestException:
        time.sleep(2)
        r = requests.get(url, headers=headers, params=params, timeout=20)

    if r.status_code == 401:
        try:
            payload = r.json()
        except Exception:
            payload = None

        if isinstance(payload, dict) and payload.get("message") == "Authorization Error":
            refresh_access_token()
            token = os.getenv("STRAVA_ACCESS_TOKEN")
            headers = {"Authorization": f"Bearer {token}"}
            r = requests.get(url, headers=headers, params=params, timeout=20)

    return r


# -----------------------
# SQLite
# -----------------------
def ensure_laps_strava_table(db_path: str = "running.db") -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS laps_strava (
        activity_id INTEGER NOT NULL,
        lap_index INTEGER NOT NULL,
        name TEXT,
        elapsed_time_s INTEGER,
        moving_time_s INTEGER,
        distance_m REAL,
        start_index INTEGER,
        end_index INTEGER,
        average_speed_m_s REAL,
        average_heartrate REAL,
        max_heartrate REAL,
        split INTEGER,
        PRIMARY KEY (activity_id, lap_index)
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_laps_strava_act ON laps_strava(activity_id);")
    conn.commit()
    conn.close()


def fetch_strava_laps(activity_id: int) -> List[Dict[str, Any]]:
    # Endpoint officiel: GET /activities/{id}/laps :contentReference[oaicite:2]{index=2}
    url = f"{STRAVA_API_BASE}/activities/{activity_id}/laps"
    r = _strava_get(url)
    if r.status_code != 200:
        try:
            data = r.json()
        except Exception:
            data = r.text
        raise RuntimeError(f"Erreur Strava laps (HTTP {r.status_code}): {data}")
    return r.json()


def store_strava_laps(activity_id: int, laps: List[Dict[str, Any]], db_path: str = "running.db") -> int:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # idempotent
    cur.execute("DELETE FROM laps_strava WHERE activity_id = ?", (activity_id,))

    rows: List[Tuple] = []
    for i, lap in enumerate(laps, start=1):
        # lap["lap_index"] existe parfois, sinon on reconstruit un index
        lap_index = int(lap.get("lap_index") or i)

        rows.append((
            activity_id,
            lap_index,
            lap.get("name"),
            lap.get("elapsed_time"),
            lap.get("moving_time"),
            lap.get("distance"),
            lap.get("start_index"),
            lap.get("end_index"),
            lap.get("average_speed"),
            lap.get("average_heartrate"),
            lap.get("max_heartrate"),
            lap.get("split"),
        ))

    cur.executemany("""
        INSERT INTO laps_strava (
            activity_id, lap_index, name, elapsed_time_s, moving_time_s, distance_m,
            start_index, end_index, average_speed_m_s, average_heartrate, max_heartrate, split
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, rows)

    conn.commit()
    conn.close()
    return len(rows)


def list_recent_runs_with_streams(db_path: str = "running.db", limit: int = 20) -> List[Tuple]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type
        FROM activities
        WHERE sport_type IN ('Run','Trail Run') AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT ?;
    """, (limit,))
    acts = cur.fetchall()

    out = []
    for (activity_id, start_date_local, name, sport_type) in acts:
        cur.execute("""
            SELECT time_s, distance_m
            FROM stream_points
            WHERE activity_id = ?
            ORDER BY idx DESC
            LIMIT 1;
        """, (activity_id,))
        last = cur.fetchone()
        if not last:
            continue
        t_s, d_m = last
        duration_min = (t_s / 60.0) if t_s is not None else None
        distance_km = (d_m / 1000.0) if d_m is not None else None
        out.append((activity_id, start_date_local, name, sport_type, duration_min, distance_km))

    conn.close()
    return out


def main():
    db_path = "running.db"
    ensure_laps_strava_table(db_path)

    rows = list_recent_runs_with_streams(db_path, limit=20)
    print("\nDernières activités RUN/TRAIL avec streams:")
    print("activity_id | date | distance_km | durée_min | sport_type | name")
    for (aid, dt, name, st, dur_min, dist_km) in rows:
        dist_txt = f"{dist_km:.2f}" if isinstance(dist_km, (int, float)) else "?"
        dur_txt = f"{dur_min:.1f}" if isinstance(dur_min, (int, float)) else "?"
        print(f"{aid} | {dt} | {dist_txt} | {dur_txt} | {st} | {name}")

    print("\nChoisis un activity_id (copie-colle le nombre).")
    activity_id = int(input("activity_id = ").strip())

    laps = fetch_strava_laps(activity_id)
    n = store_strava_laps(activity_id, laps, db_path=db_path)

    print(f"\n[OK] {n} laps Strava importés dans laps_strava pour activity_id={activity_id}")

    # aperçu
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT lap_index, name, elapsed_time_s, distance_m, average_speed_m_s, average_heartrate
        FROM laps_strava
        WHERE activity_id = ?
        ORDER BY lap_index
        LIMIT 10;
    """, (activity_id,))
    print("\nAperçu (10 premiers):")
    for r in cur.fetchall():
        print(r)
    conn.close()


if __name__ == "__main__":
    main()
