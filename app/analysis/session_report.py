import sqlite3
from typing import List, Tuple, Optional, Dict, Any
import math

DB_PATH = "running.db"


# ----------------------------
# Helpers format
# ----------------------------
def fmt_pace(pace_s_per_km: Optional[float]) -> str:
    if pace_s_per_km is None or pace_s_per_km <= 0:
        return "—"
    total = int(round(pace_s_per_km))
    mm = total // 60
    ss = total % 60
    return f"{mm}:{ss:02d}/km"


def fmt_time_s(sec: Optional[int]) -> str:
    if sec is None:
        return "—"
    sec = int(sec)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def mean(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def std(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return 0.0 if vals else None
    m = sum(vals) / len(vals)
    var = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
    return math.sqrt(var)


def pace_from_time_distance(elapsed_s: int, dist_m: float) -> Optional[float]:
    if dist_m is None or dist_m <= 0:
        return None
    return (elapsed_s / dist_m) * 1000.0


def classify_role(tag: Optional[str], class_label: Optional[str]) -> str:
    """
    Rôle "coach-like" simple:
    - RECUP si tag contient 'recup'
    - WORK si class_label indique effort, ou tag commence par 'set_' / 'strides_'
    - OTHER sinon
    """
    t = (tag or "").lower()
    if "recup" in t:
        return "RECUP"
    if t.startswith("set_") or t.startswith("strides_"):
        return "WORK"
    if (class_label or "").upper().startswith("EFFORT"):
        return "WORK"
    return "OTHER"


# ----------------------------
# DB access
# ----------------------------
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


def fetch_activity_meta(conn: sqlite3.Connection, activity_id: int) -> Optional[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type, device_name, has_heartrate
        FROM activities
        WHERE activity_id = ?;
    """, (activity_id,))
    return cur.fetchone()


def fetch_tagged_laps_detailed(conn: sqlite3.Connection, activity_id: int) -> List[Tuple]:
    """
    Une ligne par lap taggé:
      block, tag, lap_index, elapsed_time_s, distance_m, avg_speed_m_s,
      class_label, start_index, end_index
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COALESCE(t.block, 'UNSPEC') AS block,
            t.tag,
            t.lap_index,
            l.elapsed_time_s,
            l.distance_m,
            l.average_speed_m_s,
            c.class_label,
            l.start_index,
            l.end_index
        FROM lap_tags t
        JOIN laps_strava l
          ON l.activity_id = t.activity_id AND l.lap_index = t.lap_index
        LEFT JOIN laps_strava_classified c
          ON c.activity_id = l.activity_id AND c.lap_index = l.lap_index
        WHERE t.activity_id = ? AND t.source='STRAVA_LAP'
        ORDER BY
            CASE COALESCE(t.block,'UNSPEC')
                WHEN 'WARMUP' THEN 1
                WHEN 'MAIN' THEN 2
                WHEN 'COOLDOWN' THEN 3
                ELSE 9
            END,
            t.tag ASC,
            t.lap_index ASC;
    """, (activity_id,))
    return cur.fetchall()


def fetch_hr_avg_from_stream(conn: sqlite3.Connection, activity_id: int, start_idx: Optional[int], end_idx: Optional[int]) -> Optional[float]:
    if start_idx is None or end_idx is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        SELECT AVG(heartrate_bpm)
        FROM stream_points
        WHERE activity_id = ?
          AND idx BETWEEN ? AND ?
          AND heartrate_bpm IS NOT NULL;
    """, (activity_id, int(start_idx), int(end_idx)))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return float(row[0])


# ----------------------------
# Report
# ----------------------------
def build_report(activity_id: int, conn: sqlite3.Connection) -> None:
    meta = fetch_activity_meta(conn, activity_id)
    if not meta:
        print("Activité introuvable en base.")
        return

    _, dt, name, sport_type, device_name, has_hr = meta

    laps = fetch_tagged_laps_detailed(conn, activity_id)
    if not laps:
        print("\nAucun lap taggé pour cette activité.")
        print("-> Utilise d’abord: python -m app.analysis.tag_laps (Choix 3 ou tagging manuel)")
        return

    print("\n" + "=" * 72)
    print("SESSION REPORT (structuré)")
    print("=" * 72)
    print(f"activity_id : {activity_id}")
    print(f"date        : {dt}")
    print(f"name        : {name}")
    print(f"sport_type  : {sport_type}")
    print(f"device      : {device_name}")
    print(f"has_hr      : {has_hr}")
    print("-" * 72)

    # group by block -> tag
    by_block: Dict[str, Dict[str, List[Tuple]]] = {}
    for row in laps:
        block, tag, lap_index, elapsed_s, dist_m, v_m_s, class_label, s_idx, e_idx = row
        by_block.setdefault(block, {}).setdefault(tag, []).append(row)

    block_order = ["WARMUP", "MAIN", "COOLDOWN", "UNSPEC"]

    # Totaux séance (sur laps taggés)
    total_work_s = 0
    total_work_m = 0.0
    total_rec_s = 0
    total_rec_m = 0.0
    total_other_s = 0
    total_other_m = 0.0

    # Totaux MAIN uniquement (souvent le plus pertinent)
    main_work_s = 0
    main_rec_s = 0

    for block in block_order:
        if block not in by_block:
            continue

        print(f"\n[{block}]")
        tags = by_block[block]

        # tri tags: par durée totale desc
        tag_items = []
        for tag, rows in tags.items():
            tot_s = sum(int(r[3] or 0) for r in rows)
            tag_items.append((tag, tot_s))
        tag_items.sort(key=lambda x: -x[1])

        for tag, _ in tag_items:
            rows = tags[tag]

            # per-lap metrics
            lap_lines = []
            lap_paces = []
            lap_hrs = []
            tag_total_s = 0
            tag_total_m = 0.0

            # determine role
            # take first non-null class label as reference
            class_ref = None
            for r in rows:
                if r[6] is not None:
                    class_ref = r[6]
                    break
            role = classify_role(tag, class_ref)

            for (blk, tg, lap_index, elapsed_s, dist_m, v_m_s, class_label, s_idx, e_idx) in rows:
                if elapsed_s is None or dist_m is None:
                    continue
                elapsed_s = int(elapsed_s)
                dist_m = float(dist_m)
                tag_total_s += elapsed_s
                tag_total_m += dist_m

                pace = pace_from_time_distance(elapsed_s, dist_m)
                lap_paces.append(pace if pace is not None else None)

                hr = fetch_hr_avg_from_stream(conn, activity_id, s_idx, e_idx)
                lap_hrs.append(hr if hr is not None else None)

                lap_lines.append((
                    int(lap_index),
                    elapsed_s,
                    dist_m,
                    pace,
                    hr,
                    class_label
                ))

            # aggregates
            tag_pace = pace_from_time_distance(tag_total_s, tag_total_m) if tag_total_m > 0 else None
            tag_hr = mean([x for x in lap_hrs if x is not None])
            tag_pace_std = std([x for x in lap_paces if x is not None])

            # HR drift (first vs last lap in this tag)
            hr_drift = None
            hrs_clean = [x for x in lap_hrs if x is not None]
            if len(hrs_clean) >= 2:
                hr_drift = hrs_clean[-1] - hrs_clean[0]

            # Update totals (on laps taggés)
            if role == "WORK":
                total_work_s += tag_total_s
                total_work_m += tag_total_m
                if block == "MAIN":
                    main_work_s += tag_total_s
            elif role == "RECUP":
                total_rec_s += tag_total_s
                total_rec_m += tag_total_m
                if block == "MAIN":
                    main_rec_s += tag_total_s
            else:
                total_other_s += tag_total_s
                total_other_m += tag_total_m

            # Print tag header
            print(f"\n  - {tag}  [{role}]")
            print(f"    total : {fmt_time_s(tag_total_s)} | {tag_total_m/1000.0:.3f} km | pace {fmt_pace(tag_pace)}"
                  + (f" | HR {tag_hr:.1f}" if tag_hr is not None else " | HR —")
                  + (f" | pace_std {tag_pace_std:.1f}s/km" if tag_pace_std is not None else "")
                  + (f" | HR_drift {hr_drift:+.1f}" if hr_drift is not None else ""))

            # Print laps "mis en relief"
            print("    laps  : lap | dur | dist | pace | HR | class")
            for (lap_i, elapsed_s, dist_m, pace, hr, class_label) in lap_lines:
                hr_txt = f"{hr:.1f}" if hr is not None else "—"
                cl_txt = class_label if class_label is not None else "—"
                print(f"           {lap_i:>3} | {fmt_time_s(elapsed_s):>6} | {dist_m:>6.1f}m | {fmt_pace(pace):>7} | {hr_txt:>5} | {cl_txt}")

    # Synthèse (sur laps taggés)
    print("\n" + "-" * 72)
    print("SYNTHÈSE (sur laps taggés)")
    print("-" * 72)

    tagged_total_s = total_work_s + total_rec_s + total_other_s
    tagged_total_m = total_work_m + total_rec_m + total_other_m

    print(f"Total taggé      : {fmt_time_s(tagged_total_s)} | {tagged_total_m/1000.0:.3f} km")
    print(f"Travail (WORK)   : {fmt_time_s(total_work_s)} | {total_work_m/1000.0:.3f} km")
    print(f"Récup (RECUP)    : {fmt_time_s(total_rec_s)} | {total_rec_m/1000.0:.3f} km")
    print(f"Autre (OTHER)    : {fmt_time_s(total_other_s)} | {total_other_m/1000.0:.3f} km")

    if total_work_s > 0:
        density = total_work_s / max(1, total_work_s + total_rec_s)
        print(f"Densité travail  : {density*100.0:.1f}% (work / (work+recup))")
    else:
        print("Densité travail  : —")

    if main_work_s + main_rec_s > 0:
        main_density = main_work_s / max(1, main_work_s + main_rec_s)
        print(f"Densité MAIN     : {main_density*100.0:.1f}% (MAIN work / (MAIN work+recup))")
    else:
        print("Densité MAIN     : —")

    print("=" * 72)
    print("")


def main():
    conn = sqlite3.connect(DB_PATH)

    print("\nDernières activités RUN/TRAIL (streams OK):")
    for (aid, dt, name) in list_recent_runs(conn, 20):
        print(f"{aid} | {dt} | {name}")

    activity_id = int(input("\nactivity_id = ").strip())
    build_report(activity_id, conn)

    conn.close()


if __name__ == "__main__":
    main()
