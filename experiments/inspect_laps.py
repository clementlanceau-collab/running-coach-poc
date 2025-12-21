import sqlite3
from typing import List, Tuple, Optional

DB_PATH = "running.db"


def list_recent_runs(conn: sqlite3.Connection, limit: int = 15) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type
        FROM activities
        WHERE sport_type IN ('Run','Trail Run') AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT ?;
    """, (limit,))
    return cur.fetchall()


def fetch_laps(conn: sqlite3.Connection, activity_id: int) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT lap_index, name, elapsed_time_s, distance_m, average_speed_m_s, average_heartrate
        FROM laps_strava
        WHERE activity_id = ?
        ORDER BY lap_index ASC;
    """, (activity_id,))
    return cur.fetchall()


def fetch_activity_meta(conn: sqlite3.Connection, activity_id: int) -> Optional[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, device_name, has_heartrate
        FROM activities
        WHERE activity_id = ?;
    """, (activity_id,))
    return cur.fetchone()


def main():
    conn = sqlite3.connect(DB_PATH)

    print("\nDernières activités RUN/TRAIL (streams OK):")
    for (aid, dt, name, st) in list_recent_runs(conn, 15):
        print(f"{aid} | {dt} | {st} | {name}")

    raw = input("\nactivity_id (ex: 16769979439) = ").strip()
    if not raw:
        print("Erreur: activity_id requis.")
        return
    activity_id = int(raw)

    meta = fetch_activity_meta(conn, activity_id)
    if not meta:
        print("Erreur: activité introuvable.")
        return

    print("\nMETA:")
    print(meta)

    laps = fetch_laps(conn, activity_id)
    if not laps:
        print("\nAucun lap Strava trouvé (table laps_strava vide pour cette activité).")
        print("-> Relance la sync (ou vérifie laps_strava est bien alimentée).")
        return

    print("\nLAPS STRAVA:")
    print("lap | name | elapsed_s | dist_m | v_m_s | avg_hr")
    for r in laps:
        print(r)

    conn.close()


if __name__ == "__main__":
    main()
