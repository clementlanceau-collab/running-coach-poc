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


# -----------------------
# OAuth + HTTP
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
        r = requests.get(url, headers=headers, params=params, timeout=30)
    except requests.exceptions.RequestException:
        time.sleep(2)
        r = requests.get(url, headers=headers, params=params, timeout=30)

    if r.status_code == 401:
        try:
            payload = r.json()
        except Exception:
            payload = None

        if isinstance(payload, dict) and payload.get("message") == "Authorization Error":
            refresh_access_token()
            token = os.getenv("STRAVA_ACCESS_TOKEN")
            headers = {"Authorization": f"Bearer {token}"}
            r = requests.get(url, headers=headers, params=params, timeout=30)

    return r


# -----------------------
# SQLite schema
# -----------------------
def init_db(db_path: str = DB_PATH) -> None:
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
        description TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stream_points (
        activity_id INTEGER NOT NULL,
        idx INTEGER NOT NULL,
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
    CREATE TABLE IF NOT EXISTS laps_strava (
        activity_id INTEGER NOT NULL,
        lap_index INTEGER NOT NULL,
        name TEXT,
        elapsed_time_s INTEGER,
        distance_m REAL,
        average_speed_m_s REAL,
        average_heartrate REAL,
        start_index INTEGER,
        end_index INTEGER,
        PRIMARY KEY (activity_id, lap_index)
    );
    """)

    conn.commit()
    conn.close()


# -----------------------
# DB upserts
# -----------------------
def upsert_activity(conn: sqlite3.Connection, a: Dict[str, Any], description: Optional[str]) -> None:
    activity_id = int(a["id"])
    name = a.get("name")
    typ = a.get("type")
    sport_type = a.get("sport_type") or a.get("type")
    start_date_local = a.get("start_date_local") or a.get("start_date")
    manual = 1 if a.get("manual") else 0
    trainer = 1 if a.get("trainer") else 0
    device_name = a.get("device_name")
    has_heartrate = 1 if a.get("has_heartrate") else 0

    # streams_status sera mis à jour plus tard (OK / NO_STREAMS / ERROR)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO activities (
            activity_id, name, type, sport_type, start_date_local,
            manual, trainer, device_name, has_heartrate, streams_status, description
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT streams_status FROM activities WHERE activity_id=?), NULL), ?)
        ON CONFLICT(activity_id) DO UPDATE SET
            name=excluded.name,
            type=excluded.type,
            sport_type=excluded.sport_type,
            start_date_local=excluded.start_date_local,
            manual=excluded.manual,
            trainer=excluded.trainer,
            device_name=excluded.device_name,
            has_heartrate=excluded.has_heartrate,
            description=excluded.description;
    """, (
        activity_id, name, typ, sport_type, start_date_local,
        manual, trainer, device_name, has_heartrate, activity_id, description
    ))
    conn.commit()


def set_streams_status(conn: sqlite3.Connection, activity_id: int, status: str) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE activities SET streams_status=? WHERE activity_id=?", (status, activity_id))
    conn.commit()


def replace_stream_points(conn: sqlite3.Connection, activity_id: int, rows: List[Tuple]) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM stream_points WHERE activity_id=?", (activity_id,))
    cur.executemany("""
        INSERT INTO stream_points(
            activity_id, idx, time_s, distance_m, altitude_m, velocity_m_s,
            heartrate_bpm, cadence_rpm, grade, lat, lng
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, rows)
    conn.commit()


def replace_laps_strava(conn: sqlite3.Connection, activity_id: int, laps: List[Dict[str, Any]]) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM laps_strava WHERE activity_id=?", (activity_id,))
    rows = []
    for i, lap in enumerate(laps, start=1):
        rows.append((
            activity_id,
            i,
            lap.get("name"),
            lap.get("elapsed_time"),
            lap.get("distance"),
            lap.get("average_speed"),
            lap.get("average_heartrate"),
            lap.get("start_index"),
            lap.get("end_index"),
        ))
    cur.executemany("""
        INSERT INTO laps_strava(
            activity_id, lap_index, name, elapsed_time_s, distance_m,
            average_speed_m_s, average_heartrate, start_index, end_index
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, rows)
    conn.commit()
    return len(rows)


# -----------------------
# Strava fetchers
# -----------------------
def fetch_activity_detail(activity_id: int) -> Dict[str, Any]:
    url = f"{STRAVA_API_BASE}/activities/{activity_id}"
    r = _strava_get(url, params={"include_all_efforts": "true"})
    if r.status_code != 200:
        raise RuntimeError(f"Erreur GET activity detail {activity_id}: {r.status_code} {r.text}")
    return r.json()


def fetch_activity_laps(activity_id: int) -> List[Dict[str, Any]]:
    url = f"{STRAVA_API_BASE}/activities/{activity_id}/laps"
    r = _strava_get(url, params={"per_page": 200})
    if r.status_code != 200:
        # pas critique
        return []
    data = r.json()
    return data if isinstance(data, list) else []


def fetch_activity_streams(activity_id: int) -> Optional[Dict[str, Any]]:
    url = f"{STRAVA_API_BASE}/activities/{activity_id}/streams"
    keys = "time,distance,velocity_smooth,heartrate,altitude,cadence,grade_smooth,latlng"
    r = _strava_get(url, params={"keys": keys, "key_by_type": "true"})
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Erreur GET streams {activity_id}: {r.status_code} {r.text}")
    return r.json()


def build_stream_rows(activity_id: int, streams: Dict[str, Any]) -> List[Tuple]:
    # Streams key_by_type -> dict: time, distance, velocity_smooth, heartrate...
    time_arr = streams.get("time", {}).get("data") or []
    dist_arr = streams.get("distance", {}).get("data") or []
    vel_arr = streams.get("velocity_smooth", {}).get("data") or []
    hr_arr = streams.get("heartrate", {}).get("data") or []
    alt_arr = streams.get("altitude", {}).get("data") or []
    cad_arr = streams.get("cadence", {}).get("data") or []
    grd_arr = streams.get("grade_smooth", {}).get("data") or []
    latlng_arr = streams.get("latlng", {}).get("data") or []

    n = len(time_arr)
    if n == 0:
        return []

    def get(arr, i):
        return arr[i] if i < len(arr) else None

    rows = []
    for i in range(n):
        lat = None
        lng = None
        ll = get(latlng_arr, i)
        if isinstance(ll, (list, tuple)) and len(ll) == 2:
            lat, lng = ll[0], ll[1]

        rows.append((
            activity_id,
            i,
            int(get(time_arr, i)) if get(time_arr, i) is not None else None,
            float(get(dist_arr, i)) if get(dist_arr, i) is not None else None,
            float(get(alt_arr, i)) if get(alt_arr, i) is not None else None,
            float(get(vel_arr, i)) if get(vel_arr, i) is not None else None,
            int(get(hr_arr, i)) if get(hr_arr, i) is not None else None,
            float(get(cad_arr, i)) if get(cad_arr, i) is not None else None,
            float(get(grd_arr, i)) if get(grd_arr, i) is not None else None,
            float(lat) if lat is not None else None,
            float(lng) if lng is not None else None,
        ))
    return rows


def fetch_recent_activities(per_page: int = 30) -> List[Dict[str, Any]]:
    r = _strava_get(STRAVA_ACTIVITIES_URL, params={"per_page": per_page, "page": 1})
    if r.status_code != 200:
        raise RuntimeError(f"Erreur GET athlete/activities: {r.status_code} {r.text}")
    data = r.json()
    return data if isinstance(data, list) else []


# -----------------------
# Main sync
# -----------------------
def sync(per_page: int = 30, db_path: str = DB_PATH) -> None:
    init_db(db_path)
    acts = fetch_recent_activities(per_page=per_page)

    conn = sqlite3.connect(db_path)

    ok = 0
    no_streams = 0
    errors = 0

    for a in acts:
        activity_id = int(a["id"])
        sport_type = a.get("sport_type") or a.get("type")

        # detail pour description/device_name parfois plus fiable
        try:
            detail = fetch_activity_detail(activity_id)
            description = detail.get("description")
            # merge: on garde fields du listing + champs détaillés si dispo
            a_merged = dict(a)
            for k in ["device_name", "has_heartrate", "manual", "trainer", "sport_type", "type", "start_date_local", "name"]:
                if detail.get(k) is not None:
                    a_merged[k] = detail.get(k)
            upsert_activity(conn, a_merged, description)
        except Exception as e:
            # On stocke au moins l'activité "listing"
            upsert_activity(conn, a, None)

        # Laps strava (indépendant des streams)
        laps = fetch_activity_laps(activity_id)
        n_laps = replace_laps_strava(conn, activity_id, laps)

        # Streams seulement si non-manuel (sinon 404 ou vide)
        manual = 1 if (a.get("manual") or False) else 0
        if manual == 1:
            set_streams_status(conn, activity_id, "NO_STREAMS")
            no_streams += 1
            print(f"[NO_STREAMS] {activity_id} ({sport_type}) | manual=1 | laps={n_laps}")
            continue

        try:
            streams = fetch_activity_streams(activity_id)
            if not streams:
                set_streams_status(conn, activity_id, "NO_STREAMS")
                no_streams += 1
                print(f"[NO_STREAMS] {activity_id} ({sport_type}) | streams=None | laps={n_laps}")
                continue

            rows = build_stream_rows(activity_id, streams)
            if not rows:
                set_streams_status(conn, activity_id, "NO_STREAMS")
                no_streams += 1
                print(f"[NO_STREAMS] {activity_id} ({sport_type}) | streams empty | laps={n_laps}")
                continue

            replace_stream_points(conn, activity_id, rows)
            set_streams_status(conn, activity_id, "OK")
            ok += 1
            print(f"[OK] {activity_id} ({sport_type}) | points={len(rows)} | laps={n_laps}")

        except Exception as e:
            set_streams_status(conn, activity_id, "ERROR")
            errors += 1
            print(f"[ERROR] {activity_id} ({sport_type}) | {e}")

    conn.close()
    print(f"\nSYNC DONE: OK={ok} | NO_STREAMS={no_streams} | ERROR={errors} | total={len(acts)}")


def main():
    # Tu peux monter à 50 sans souci
    sync(per_page=50, db_path=DB_PATH)


if __name__ == "__main__":
    main()
