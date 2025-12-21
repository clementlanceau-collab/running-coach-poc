import sqlite3
from typing import Optional, Dict, Any, List, Tuple
import math

DB_PATH = "running.db"


# -----------------------
# Formatting helpers
# -----------------------
def fmt_pace(pace_s_per_km: Optional[float]) -> str:
    if pace_s_per_km is None or pace_s_per_km <= 0:
        return "—"
    total = int(round(pace_s_per_km))
    mm = total // 60
    ss = total % 60
    return f"{mm}:{ss:02d}/km"


def fmt_float(x: Optional[float], nd: int = 1) -> str:
    if x is None:
        return "—"
    return f"{x:.{nd}f}"


def fmt_mmss(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    s = int(round(seconds))
    mm = s // 60
    ss = s % 60
    return f"{mm}:{ss:02d}"


def mean(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def weighted_mean(pairs: List[Tuple[Optional[float], float]]) -> Optional[float]:
    """
    pairs: (value, weight) where weight >= 0
    """
    num = 0.0
    den = 0.0
    for v, w in pairs:
        if v is None:
            continue
        if w is None or w <= 0:
            continue
        num += float(v) * float(w)
        den += float(w)
    if den <= 0:
        return None
    return num / den


def stdev_sample(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return 0.0 if len(vals) == 1 else None
    m = sum(vals) / len(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (len(vals) - 1))


def pace_from_time_distance(elapsed_s: int, dist_m: float) -> Optional[float]:
    if dist_m is None or dist_m <= 0:
        return None
    return (elapsed_s / dist_m) * 1000.0


# -----------------------
# Domain classification
# -----------------------
def normalize_block(block: Optional[str]) -> str:
    b = (block or "").strip().upper()
    if b in ("WARMUP", "MAIN", "COOLDOWN"):
        return b
    if b in ("PAUSE", "STOP", "TRANSITION"):
        return "PAUSE"
    if b == "UNSPEC" or b == "":
        return "UNSPEC"
    return b


def classify_role(tag: str) -> str:
    """
    Roles reflect intent based on tag naming (declared structure),
    not measured intensity.
    """
    t = (tag or "").lower().strip()
    if "recup" in t:
        return "RECUP"
    if t.startswith("set_") or t.startswith("strides_"):
        return "WORK"
    if "pause" in t or "stop" in t or "transition" in t:
        return "PAUSE"
    return "OTHER"


# -----------------------
# DB helpers
# -----------------------
def fetch_activity_meta(conn: sqlite3.Connection, activity_id: int) -> Optional[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type, device_name, has_heartrate
        FROM activities
        WHERE activity_id = ?;
    """, (activity_id,))
    return cur.fetchone()


def ensure_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?;
    """, (table_name,))
    return cur.fetchone() is not None


def fetch_main_tagged_laps(conn: sqlite3.Connection, activity_id: int) -> List[Tuple]:
    """
    Returns rows:
    tag, block, lap_index, elapsed_s, dist_m, start_index, end_index
    Filtered to block == MAIN (after normalization) and source == STRAVA_LAP.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            t.tag,
            COALESCE(t.block,'UNSPEC') AS block,
            t.lap_index,
            l.elapsed_time_s,
            l.distance_m,
            l.start_index,
            l.end_index
        FROM lap_tags t
        JOIN laps_strava l
          ON l.activity_id = t.activity_id AND l.lap_index = t.lap_index
        WHERE t.activity_id = ?
          AND t.source = 'STRAVA_LAP'
        ORDER BY t.lap_index;
    """, (activity_id,))
    rows = cur.fetchall()

    out = []
    for (tag, block, lap_index, elapsed_s, dist_m, s_idx, e_idx) in rows:
        nb = normalize_block(block)
        if nb == "MAIN":
            out.append((tag, nb, lap_index, elapsed_s, dist_m, s_idx, e_idx))
    return out


def hr_avg_from_stream(conn: sqlite3.Connection, activity_id: int,
                       start_idx: Optional[int], end_idx: Optional[int]) -> Optional[float]:
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


# -----------------------
# MAIN summary computation
# -----------------------
def compute_main_summary(conn: sqlite3.Connection, activity_id: int) -> Dict[str, Any]:
    rows = fetch_main_tagged_laps(conn, activity_id)

    by_tag: Dict[str, List[Tuple]] = {}
    for (tag, block, lap_index, elapsed_s, dist_m, s_idx, e_idx) in rows:
        by_tag.setdefault(tag, []).append((lap_index, elapsed_s, dist_m, s_idx, e_idx))

    totals = {
        "WORK_s": 0.0, "WORK_m": 0.0,
        "RECUP_s": 0.0, "RECUP_m": 0.0,
        "OTHER_s": 0.0, "OTHER_m": 0.0,
        "PAUSE_s": 0.0, "PAUSE_m": 0.0
    }

    tag_metrics: Dict[str, Dict[str, Any]] = {}

    # Weighted aggregated indicators (WORK tags only), weight by total_s of tag
    work_pace_pairs: List[Tuple[Optional[float], float]] = []
    work_hr_pairs: List[Tuple[Optional[float], float]] = []
    work_pace_std_pairs: List[Tuple[Optional[float], float]] = []
    work_hr_drift_pairs: List[Tuple[Optional[float], float]] = []

    for tag, laps in by_tag.items():
        laps_sorted = sorted(laps, key=lambda x: x[0])
        role = classify_role(tag)

        total_s = 0
        total_m = 0.0
        lap_paces: List[float] = []
        lap_hrs: List[float] = []

        for (lap_index, elapsed_s, dist_m, s_idx, e_idx) in laps_sorted:
            if elapsed_s is None or dist_m is None:
                continue
            elapsed_s = int(elapsed_s)
            dist_m = float(dist_m)

            total_s += elapsed_s
            total_m += dist_m

            p = pace_from_time_distance(elapsed_s, dist_m)
            if p is not None:
                lap_paces.append(p)

            hr = hr_avg_from_stream(conn, activity_id, s_idx, e_idx)
            if hr is not None:
                lap_hrs.append(hr)

        pace = pace_from_time_distance(total_s, total_m) if total_m > 0 else None
        hr = mean(lap_hrs)
        pace_std = stdev_sample(lap_paces)

        hr_drift = None
        if len(lap_hrs) >= 2:
            hr_drift = lap_hrs[-1] - lap_hrs[0]

        # totals by role
        if role == "WORK":
            totals["WORK_s"] += total_s
            totals["WORK_m"] += total_m
        elif role == "RECUP":
            totals["RECUP_s"] += total_s
            totals["RECUP_m"] += total_m
        elif role == "PAUSE":
            totals["PAUSE_s"] += total_s
            totals["PAUSE_m"] += total_m
        else:
            totals["OTHER_s"] += total_s
            totals["OTHER_m"] += total_m

        tag_metrics[tag] = {
            "tag": tag,
            "role": role,
            "n_laps": len(laps_sorted),
            "total_s": total_s,
            "total_m": total_m,
            "pace": pace,
            "hr": hr,
            "pace_std": pace_std,
            "hr_drift": hr_drift
        }

        if role == "WORK" and total_s > 0:
            w = float(total_s)
            work_pace_pairs.append((pace, w))
            work_hr_pairs.append((hr, w))
            work_pace_std_pairs.append((pace_std, w))
            work_hr_drift_pairs.append((hr_drift, w))

    density = None
    if (totals["WORK_s"] + totals["RECUP_s"]) > 0:
        density = totals["WORK_s"] / (totals["WORK_s"] + totals["RECUP_s"])

    work_pace = weighted_mean(work_pace_pairs)
    work_hr = weighted_mean(work_hr_pairs)
    work_pace_std_mean = weighted_mean(work_pace_std_pairs)
    work_hr_drift_mean = weighted_mean(work_hr_drift_pairs)

    return {
        "activity_id": activity_id,
        "totals": totals,
        "density": density,
        "work_indicators": {
            "work_pace_mean": work_pace,
            "work_hr_mean": work_hr,
            "work_pace_std_mean": work_pace_std_mean,
            "work_hr_drift_mean": work_hr_drift_mean
        },
        "tag_metrics": tag_metrics
    }


# -----------------------
# Printing
# -----------------------
def print_main_summary(meta: Tuple, summary: Dict[str, Any]) -> None:
    activity_id, date_local, name, sport_type, device_name, has_hr = meta

    print("\n" + "=" * 88)
    print("MAIN SUMMARY (fiche MAIN, factuel, sans matching strict)")
    print("=" * 88)
    print(f"activity_id : {activity_id}")
    print(f"date        : {date_local}")
    print(f"name        : {name}")
    print(f"sport_type  : {sport_type}")
    print(f"device      : {device_name}")
    print(f"has_hr      : {has_hr}")
    print("-" * 88)

    t = summary["totals"]
    d = summary["density"]
    wi = summary["work_indicators"]

    print("[MAIN volumes]")
    print(f"  WORK  : {fmt_mmss(t['WORK_s'])} | {t['WORK_m']/1000.0:.3f} km")
    print(f"  RECUP : {fmt_mmss(t['RECUP_s'])} | {t['RECUP_m']/1000.0:.3f} km")
    print(f"  OTHER : {fmt_mmss(t['OTHER_s'])} | {t['OTHER_m']/1000.0:.3f} km")
    if t["PAUSE_s"] > 0:
        print(f"  PAUSE : {fmt_mmss(t['PAUSE_s'])} | {t['PAUSE_m']/1000.0:.3f} km")
    print(f"  Densité (WORK/(WORK+RECUP)) : {fmt_float(d*100.0 if d is not None else None, 1)}%")

    print("\n[MAIN indicators (WORK tags, agrégé)]")
    print("  (pondérés par le temps WORK de chaque tag)")
    print(f"  pace WORK moyen (pondéré)        : {fmt_pace(wi['work_pace_mean'])}")
    print(f"  HR WORK moyen (pondéré)          : {fmt_float(wi['work_hr_mean'], 1)}")
    print(f"  stabilité pace (std, pondéré)    : {fmt_float(wi['work_pace_std_mean'], 1)} s/km")
    print(f"  dérive HR (pondéré)              : {fmt_float(wi['work_hr_drift_mean'], 1)} bpm")

    print("\n[MAIN details par tag]")
    role_rank = {"WORK": 1, "RECUP": 2, "OTHER": 3, "PAUSE": 4}

    tags_sorted = sorted(
        summary["tag_metrics"].values(),
        key=lambda x: (role_rank.get(x["role"], 9), -(x["total_s"] or 0), x["tag"])
    )

    for m in tags_sorted:
        print(f"\n  - {m['tag']} [{m['role']}]")
        print(
            f"      {m['n_laps']} laps | {m['total_s']}s | {m['total_m']:.0f}m | "
            f"pace {fmt_pace(m['pace'])} | HR {fmt_float(m['hr'],1)} | "
            f"pace_std {fmt_float(m['pace_std'],1)} | HR_drift {fmt_float(m['hr_drift'],1)}"
        )

    print("\n" + "=" * 88)
    print("")


def main() -> None:
    conn = sqlite3.connect(DB_PATH)

    if not ensure_table_exists(conn, "lap_tags"):
        print("Erreur: table lap_tags absente. Exécute d'abord le tagging.")
        return
    if not ensure_table_exists(conn, "laps_strava"):
        print("Erreur: table laps_strava absente.")
        return
    if not ensure_table_exists(conn, "stream_points"):
        print("Erreur: table stream_points absente.")
        return

    print("\nDernières activités RUN/TRAIL (streams OK):")
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name
        FROM activities
        WHERE sport_type IN ('Run','Trail Run') AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT 20;
    """)
    for r in cur.fetchall():
        print(f"{r[0]} | {r[1]} | {r[2]}")

    activity_id = int(input("\nactivity_id = ").strip())
    meta = fetch_activity_meta(conn, activity_id)
    if not meta:
        print("Erreur: activity_id invalide.")
        return

    summary = compute_main_summary(conn, activity_id)
    print_main_summary(meta, summary)

    conn.close()


if __name__ == "__main__":
    main()
