print("### classify_strava_laps.py VERSION = V2 ###")

import sqlite3
from typing import List, Tuple, Optional, Dict, Any


DB_PATH = "running.db"


def ensure_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS laps_strava_classified (
        activity_id INTEGER NOT NULL,
        lap_index INTEGER NOT NULL,
        class_label TEXT NOT NULL,      -- 'EFFORT_PROB', 'RECUP_PROB', 'OTHER', 'IGNORE'
        reason TEXT,
        PRIMARY KEY (activity_id, lap_index)
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_laps_strava_cls_act ON laps_strava_classified(activity_id);")
    conn.commit()


def fetch_laps_strava(conn: sqlite3.Connection, activity_id: int) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT lap_index, name, elapsed_time_s, distance_m, average_speed_m_s, average_heartrate
        FROM laps_strava
        WHERE activity_id = ?
        ORDER BY lap_index;
    """, (activity_id,))
    return cur.fetchall()


def median(values: List[float]) -> Optional[float]:
    vals = sorted([v for v in values if v is not None])
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    if n % 2 == 1:
        return float(vals[mid])
    return float((vals[mid - 1] + vals[mid]) / 2)


def classify_laps(activity_id: int, conn: sqlite3.Connection) -> Dict[str, Any]:
    laps = fetch_laps_strava(conn, activity_id)
    if not laps:
        return {"note": "no laps_strava", "count": 0}

    speeds = [float(l[4]) for l in laps if isinstance(l[4], (int, float)) and l[4] > 0]
    v_med = median(speeds) if speeds else 0.0

    # Paramètres
    min_valid_elapsed_s = 8
    min_valid_distance_m = 20.0

    interval_max_s = 220   # <= 3'40 (inclut 3')
    long_min_s = 240       # >= 4' : OTHER

    effort_factor = 1.15
    recup_factor = 0.90

    rows_to_insert = []
    for (lap_index, name, elapsed_s, dist_m, v_avg, hr_avg) in laps:
        if elapsed_s is None or dist_m is None or v_avg is None:
            label = "OTHER"
            reason = "missing fields"
        else:
            elapsed_s = int(elapsed_s)
            dist_m = float(dist_m)
            v_avg = float(v_avg)

            if elapsed_s < min_valid_elapsed_s or dist_m < min_valid_distance_m:
                label = "IGNORE"
                reason = f"aberrant/skip (elapsed={elapsed_s}s dist={dist_m:.1f}m)"
            elif elapsed_s >= long_min_s:
                label = "OTHER"
                reason = f"long lap ({elapsed_s}s)"
            elif elapsed_s <= interval_max_s:
                if v_avg >= v_med * effort_factor:
                    label = "EFFORT_PROB"
                    reason = f"interval({elapsed_s}s) fast(v={v_avg:.2f}>=med*{effort_factor})"
                elif v_avg <= v_med * recup_factor:
                    label = "RECUP_PROB"
                    reason = f"interval({elapsed_s}s) slow(v={v_avg:.2f}<=med*{recup_factor})"
                else:
                    label = "OTHER"
                    reason = f"interval({elapsed_s}s) mid-speed"
            else:
                label = "OTHER"
                reason = f"mid duration ({elapsed_s}s)"

        rows_to_insert.append((activity_id, int(lap_index), label, reason))

    cur = conn.cursor()
    cur.execute("DELETE FROM laps_strava_classified WHERE activity_id = ?", (activity_id,))
    cur.executemany("""
        INSERT INTO laps_strava_classified(activity_id, lap_index, class_label, reason)
        VALUES (?, ?, ?, ?);
    """, rows_to_insert)
    conn.commit()

    counts = {"EFFORT_PROB": 0, "RECUP_PROB": 0, "OTHER": 0, "IGNORE": 0}
    for _, _, label, _ in rows_to_insert:
        counts[label] += 1

    return {"v_med": v_med, "counts": counts, "params": {
        "min_valid_elapsed_s": min_valid_elapsed_s,
        "min_valid_distance_m": min_valid_distance_m,
        "interval_max_s": interval_max_s,
        "long_min_s": long_min_s,
        "effort_factor": effort_factor,
        "recup_factor": recup_factor
    }}


def print_preview(conn: sqlite3.Connection, activity_id: int, limit: int = 40) -> None:
    cur = conn.cursor()
    cur.execute("""
        SELECT l.lap_index, l.elapsed_time_s, ROUND(l.distance_m, 1), ROUND(l.average_speed_m_s, 2),
               c.class_label, c.reason
        FROM laps_strava l
        LEFT JOIN laps_strava_classified c
          ON c.activity_id = l.activity_id AND c.lap_index = l.lap_index
        WHERE l.activity_id = ?
        ORDER BY l.lap_index
        LIMIT ?;
    """, (activity_id, limit))
    rows = cur.fetchall()

    print("\nAperçu laps_strava + classification:")
    print("lap | elapsed_s | dist_m | v_m_s | class | reason")
    for r in rows:
        print(r)
    print("")


def list_recent_runs(conn: sqlite3.Connection, limit: int = 20) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name
        FROM activities
        WHERE sport_type IN ('Run','Trail Run') AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT ?;
    """, (limit,))
    return cur.fetchall()


def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    recent = list_recent_runs(conn, 20)
    print("\nDernières activités RUN/TRAIL (streams OK):")
    for (aid, dt, name) in recent:
        print(f"{aid} | {dt} | {name}")

    activity_id = int(input("\nactivity_id = ").strip())

    res = classify_laps(activity_id, conn)
    print("\nRésultat:", res)

    print_preview(conn, activity_id, limit=40)
    conn.close()


if __name__ == "__main__":
    main()
