from dotenv import load_dotenv
load_dotenv()

import os
import time
import sqlite3
import requests
from typing import Optional, Dict, Any, List, Tuple


DB_PATH = "running.db"

STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_API_BASE = "https://www.strava.com/api/v3"
STRAVA_ACTIVITIES_URL = f"{STRAVA_API_BASE}/athlete/activities"
STRAVA_STREAMS_URL = f"{STRAVA_API_BASE}/activities/{{activity_id}}/streams"
STRAVA_LAPS_URL = f"{STRAVA_API_BASE}/activities/{{activity_id}}/laps"


# -----------------------
# OAuth + HTTP
# -----------------------
def refresh_access_token() -> str:
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")

    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Variables .env manquantes: STRAVA_CLIENT_ID / STRAVA_CLIENT_SECRET / STRAVA_REFRESH_TOKEN")

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
    if not new_access:
        raise RuntimeError(f"Refresh token OK mais pas d'access_token: {data}")

    return new_access


def strava_get(url: str, access_token: str, params: Optional[Dict[str, Any]] = None) -> Any:
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Erreur Strava API {r.status_code}: {r.text}")
    return r.json()


# -----------------------
# DB schema
# -----------------------
def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS activities (
        activity_id INTEGER PRIMARY KEY,
        name TEXT,
        type TEXT,
        sport_type TEXT,
        start_date_local TEXT,
        manual INTEGER,
        trainer INTEGER,
        device_name TEXT,
        has_heartrate INTEGER,
        streams_status TEXT,
        description TEXT,
        source TEXT,
        source_activity_id TEXT
    );
    """)

    # Idempotent migration
    try:
        cur.execute("ALTER TABLE activities ADD COLUMN source TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE activities ADD COLUMN source_activity_id TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        """
        UPDATE activities
        SET source = COALESCE(source, 'STRAVA'),
            source_activity_id = COALESCE(source_activity_id, CAST(activity_id AS TEXT))
        WHERE source IS NULL OR source_activity_id IS NULL
        """
    )

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stream_points (
        activity_id INTEGER,
        idx INTEGER,
        time_s INTEGER,
        distance_m REAL,
        altitude_m REAL,
        velocity_m_s REAL,
        heartrate_bpm INTEGER,
        cadence_rpm REAL,
        grade REAL,
        lat REAL,
        lng REAL,
        PRIMARY KEY (activity_id, idx)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS laps_st (
        activity_id INTEGER,
        lap_index INTEGER,
        name TEXT,
        elapsed_time_s INTEGER,
        moving_time_s INTEGER,
        start_index INTEGER,
        end_index INTEGER,
        distance_m REAL,
        average_speed_m_s REAL,
        average_heartrate_bpm REAL,
        max_heartrate_bpm REAL,
        average_cadence_rpm REAL,
        PRIMARY KEY (activity_id, lap_index)
    );
    """)

    conn.commit()
    return conn


# -----------------------
# Insert / update helpers
# -----------------------
def upsert_activity(
    conn: sqlite3.Connection,
    activity_id: int,
    name: str,
    typ: str,
    sport_type: str,
    start_date_local: str,
    manual: int,
    trainer: int,
    device_name: str,
    has_heartrate: int,
    description: Optional[str],
) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO activities (
            activity_id, name, type, sport_type, start_date_local,
            manual, trainer, device_name, has_heartrate,
            streams_status, description,
            source, source_activity_id
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?,
            COALESCE((SELECT streams_status FROM activities WHERE activity_id=?), NULL),
            ?,
            'STRAVA', CAST(? AS TEXT)
        )
        ON CONFLICT(activity_id) DO UPDATE SET
            name=excluded.name,
            type=excluded.type,
            sport_type=excluded.sport_type,
            start_date_local=excluded.start_date_local,
            manual=excluded.manual,
            trainer=excluded.trainer,
            device_name=excluded.device_name,
            has_heartrate=excluded.has_heartrate,
            description=excluded.description,
            source=COALESCE(activities.source, excluded.source),
            source_activity_id=COALESCE(activities.source_activity_id, excluded.source_activity_id)
    """, (
        activity_id, name, typ, sport_type, start_date_local,
        manual, trainer, device_name, has_heartrate,
        activity_id, description, activity_id
    ))
    conn.commit()


def set_streams_status(conn: sqlite3.Connection, activity_id: int, status: str) -> None:
    conn.execute("UPDATE activities SET streams_status=? WHERE activity_id=?", (status, activity_id))
    conn.commit()


def clear_stream_points(conn: sqlite3.Connection, activity_id: int) -> None:
    conn.execute("DELETE FROM stream_points WHERE activity_id=?", (activity_id,))
    conn.commit()


def clear_laps(conn: sqlite3.Connection, activity_id: int) -> None:
    conn.execute("DELETE FROM laps_st WHERE activity_id=?", (activity_id,))
    conn.commit()


def insert_stream_points(conn: sqlite3.Connection, points: List[Tuple]) -> None:
    conn.executemany("""
        INSERT OR REPLACE INTO stream_points (
            activity_id, idx, time_s, distance_m, altitude_m,
            velocity_m_s, heartrate_bpm, cadence_rpm, grade, lat, lng
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, points)
    conn.commit()


def insert_laps(conn: sqlite3.Connection, activity_id: int, laps: List[Dict[str, Any]]) -> None:
    rows = []
    for i, lap in enumerate(laps):
        rows.append((
            activity_id,
            i + 1,
            lap.get("name"),
            lap.get("elapsed_time"),
            lap.get("moving_time"),
            lap.get("start_index"),
            lap.get("end_index"),
            lap.get("distance"),
            lap.get("average_speed"),
            lap.get("average_heartrate"),
            lap.get("max_heartrate"),
            lap.get("average_cadence"),
        ))
    conn.executemany("""
        INSERT OR REPLACE INTO laps_st (
            activity_id, lap_index, name, elapsed_time_s, moving_time_s,
            start_index, end_index, distance_m, average_speed_m_s,
            average_heartrate_bpm, max_heartrate_bpm, average_cadence_rpm
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()


# -----------------------
# Streams parsing
# -----------------------
def parse_streams(streams: Any) -> List[Tuple]:
    if not streams:
        return []

    by_type = {s["type"]: s["data"] for s in streams if "type" in s and "data" in s}

    times = by_type.get("time", [])
    dist = by_type.get("distance", [])
    alt = by_type.get("altitude", [])
    vel = by_type.get("velocity_smooth", [])
    hr = by_type.get("heartrate", [])
    cad = by_type.get("cadence", [])
    grade = by_type.get("grade_smooth", [])
    latlng = by_type.get("latlng", [])

    n = max(len(times), len(dist), len(alt), len(vel), len(hr), len(cad), len(grade), len(latlng))
    rows = []
    for i in range(n):
        rows.append((
            i,
            times[i] if i < len(times) else None,
            dist[i] if i < len(dist) else None,
            alt[i] if i < len(alt) else None,
            vel[i] if i < len(vel) else None,
            hr[i] if i < len(hr) else None,
            cad[i] if i < len(cad) else None,
            grade[i] if i < len(grade) else None,
            latlng[i][0] if i < len(latlng) and latlng[i] else None,
            latlng[i][1] if i < len(latlng) and latlng[i] else None,
        ))
    return rows


# -----------------------
# Main sync
# -----------------------
def sync(limit: int = 50, sleep_s: float = 0.2) -> None:
    access = refresh_access_token()
    conn = init_db(DB_PATH)

    activities = strava_get(STRAVA_ACTIVITIES_URL, access, params={"per_page": limit, "page": 1})

    ok = 0
    no_streams = 0
    err = 0

    for a in activities:
        activity_id = int(a["id"])
        name = a.get("name", "")
        typ = a.get("type", "")
        sport_type = a.get("sport_type", typ)
        start_date_local = a.get("start_date_local") or a.get("start_date") or ""
        manual = int(a.get("manual", 0) or 0)
        trainer = int(a.get("trainer", 0) or 0)
        device_name = a.get("device_name") or ""
        has_heartrate = int(a.get("has_heartrate", 0) or 0)
        description = a.get("description")

        try:
            upsert_activity(
                conn,
                activity_id,
                name,
                typ,
                sport_type,
                start_date_local,
                manual,
                trainer,
                device_name,
                has_heartrate,
                description,
            )

            if manual == 1:
                set_streams_status(conn, activity_id, "NO_STREAMS")
                no_streams += 1
                print(f"[NO_STREAMS] {activity_id} ({typ}) | manual=1 | laps=0")
                continue

            streams = strava_get(
                STRAVA_STREAMS_URL.format(activity_id=activity_id),
                access,
                params={"keys": "time,distance,altitude,velocity_smooth,heartrate,cadence,grade_smooth,latlng"},
            )
            rows = parse_streams(streams)
            if not rows:
                set_streams_status(conn, activity_id, "NO_STREAMS")
                no_streams += 1
                print(f"[NO_STREAMS] {activity_id} ({typ}) | manual=0 | laps=0")
                continue

            clear_stream_points(conn, activity_id)
            rows2 = [(activity_id, idx, t, d, alt, v, h, c, g, lat, lng)
                     for (idx, t, d, alt, v, h, c, g, lat, lng) in rows]
            insert_stream_points(conn, rows2)

            laps = strava_get(STRAVA_LAPS_URL.format(activity_id=activity_id), access)
            clear_laps(conn, activity_id)
            if isinstance(laps, list) and laps:
                insert_laps(conn, activity_id, laps)
                laps_count = len(laps)
            else:
                laps_count = 0

            set_streams_status(conn, activity_id, "OK")
            ok += 1
            print(f"[OK] {activity_id} ({typ}) | points={len(rows2)} | laps={laps_count}")

            time.sleep(sleep_s)

        except Exception as e:
            set_streams_status(conn, activity_id, "ERROR")
            err += 1
            print(f"[ERROR] {activity_id} ({typ}) -> {e}")

    conn.close()
    print(f"\nSYNC DONE: OK={ok} | NO_STREAMS={no_streams} | ERROR={err} | total={len(activities)}")


if __name__ == "__main__":
    sync(limit=50)
