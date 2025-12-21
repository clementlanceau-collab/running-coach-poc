import sqlite3
from typing import List, Tuple, Optional, Dict, Any
import math

DB_PATH = "running.db"

# -----------------------
# Format helpers
# -----------------------
def fmt_pace(pace_s_per_km: Optional[float]) -> str:
    if pace_s_per_km is None or pace_s_per_km <= 0:
        return "—"
    total = int(round(pace_s_per_km))
    mm = total // 60
    ss = total % 60
    return f"{mm}:{ss:02d}/km"


def fmt_delta_pace(a: Optional[float], b: Optional[float]) -> str:
    if a is None or b is None:
        return "—"
    d = b - a  # b minus a (positive = slower)
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.1f}s/km"


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
    Role is about intent inside a block:
    - WORK: work intervals / reps
    - RECUP: recovery intervals between reps
    - OTHER: warmup steady running, cooldown, miscellaneous
    """
    t = (tag or "").lower().strip()
    if "recup" in t:
        return "RECUP"
    if t.startswith("set_") or t.startswith("strides_"):
        return "WORK"
    if t in ("warmup", "cooldown"):
        return "OTHER"
    if "pause" in t or "stop" in t or "transition" in t:
        return "PAUSE"
    return "OTHER"


# -----------------------
# DB fetchers
# -----------------------
def fetch_activity_meta(conn: sqlite3.Connection, activity_id: int) -> Optional[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type
        FROM activities
        WHERE activity_id = ?;
    """, (activity_id,))
    return cur.fetchone()


def ensure_lap_tags_table_exists(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='lap_tags';
    """)
    return cur.fetchone() is not None


def fetch_tagged_laps(conn: sqlite3.Connection, activity_id: int) -> List[Tuple]:
    """
    tag, block, lap_index, elapsed_s, dist_m, start_index, end_index
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
        WHERE t.activity_id = ? AND t.source='STRAVA_LAP'
        ORDER BY t.lap_index;
    """, (activity_id,))
    return cur.fetchall()


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
# Metrics computation
# -----------------------
def compute_report_metrics(conn: sqlite3.Connection, activity_id: int) -> Dict[str, Any]:
    laps = fetch_tagged_laps(conn, activity_id)

    # group by (block, tag)
    by_key: Dict[Tuple[str, str], List[Tuple]] = {}
    for (tag, block, lap_index, elapsed_s, dist_m, s_idx, e_idx) in laps:
        nb = normalize_block(block)
        by_key.setdefault((nb, tag), []).append((lap_index, elapsed_s, dist_m, s_idx, e_idx))

    # metrics per (block, tag)
    key_metrics: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # totals by block and role
    totals: Dict[str, Dict[str, float]] = {}
    for b in ("WARMUP", "MAIN", "COOLDOWN", "PAUSE", "UNSPEC"):
        totals[b] = {"WORK_s": 0.0, "WORK_m": 0.0,
                     "RECUP_s": 0.0, "RECUP_m": 0.0,
                     "OTHER_s": 0.0, "OTHER_m": 0.0,
                     "PAUSE_s": 0.0, "PAUSE_m": 0.0}

    for (block, tag), rows in by_key.items():
        total_s = 0
        total_m = 0.0
        lap_paces = []
        lap_hrs = []

        # ensure stable order by lap_index inside a key
        rows_sorted = sorted(rows, key=lambda x: x[0])

        for (lap_index, elapsed_s, dist_m, s_idx, e_idx) in rows_sorted:
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

        # variability across laps inside this (block, tag)
        pace_std = None
        if len(lap_paces) >= 2:
            m = sum(lap_paces) / len(lap_paces)
            pace_std = math.sqrt(sum((x - m) ** 2 for x in lap_paces) / (len(lap_paces) - 1))
        elif len(lap_paces) == 1:
            pace_std = 0.0

        # HR drift: first vs last HR inside this (block, tag)
        hr_drift = None
        if len(lap_hrs) >= 2:
            hr_drift = lap_hrs[-1] - lap_hrs[0]

        role = classify_role(tag)

        # accumulate totals by block/role
        if role == "WORK":
            totals[block]["WORK_s"] += total_s
            totals[block]["WORK_m"] += total_m
        elif role == "RECUP":
            totals[block]["RECUP_s"] += total_s
            totals[block]["RECUP_m"] += total_m
        elif role == "PAUSE":
            totals[block]["PAUSE_s"] += total_s
            totals[block]["PAUSE_m"] += total_m
        else:
            totals[block]["OTHER_s"] += total_s
            totals[block]["OTHER_m"] += total_m

        key_metrics[(block, tag)] = {
            "block": block,
            "tag": tag,
            "role": role,
            "n_laps": len(rows_sorted),
            "total_s": total_s,
            "total_m": total_m,
            "pace": pace,
            "hr": hr,
            "pace_std": pace_std,
            "hr_drift": hr_drift
        }

    def density_for(block: str) -> Optional[float]:
        w = totals[block]["WORK_s"]
        r = totals[block]["RECUP_s"]
        if (w + r) <= 0:
            return None
        return w / (w + r)

    return {
        "activity_id": activity_id,
        "key_metrics": key_metrics,
        "totals": totals,
        "density": {b: density_for(b) for b in totals.keys()}
    }


# -----------------------
# Printing
# -----------------------
def print_block_summary(label: str, ta: Dict[str, float], tb: Dict[str, float], da: Optional[float], db: Optional[float]) -> None:
    print(f"\n[{label}]")
    print(f"  WORK  A: {fmt_mmss(ta['WORK_s'])} | {ta['WORK_m']/1000.0:.3f} km"
          f"   ||   B: {fmt_mmss(tb['WORK_s'])} | {tb['WORK_m']/1000.0:.3f} km")
    print(f"  RECUP A: {fmt_mmss(ta['RECUP_s'])} | {ta['RECUP_m']/1000.0:.3f} km"
          f"   ||   B: {fmt_mmss(tb['RECUP_s'])} | {tb['RECUP_m']/1000.0:.3f} km")
    print(f"  OTHER A: {fmt_mmss(ta['OTHER_s'])} | {ta['OTHER_m']/1000.0:.3f} km"
          f"   ||   B: {fmt_mmss(tb['OTHER_s'])} | {tb['OTHER_m']/1000.0:.3f} km")
    if ta["PAUSE_s"] > 0 or tb["PAUSE_s"] > 0:
        print(f"  PAUSE A: {fmt_mmss(ta['PAUSE_s'])} | {ta['PAUSE_m']/1000.0:.3f} km"
              f"   ||   B: {fmt_mmss(tb['PAUSE_s'])} | {tb['PAUSE_m']/1000.0:.3f} km")
    print(f"  Densité (WORK/(WORK+RECUP)) A: {fmt_float(da*100.0 if da is not None else None, 1)}%   ||   "
          f"B: {fmt_float(db*100.0 if db is not None else None, 1)}%")


def print_compare(a_meta: Tuple, b_meta: Tuple, A: Dict[str, Any], B: Dict[str, Any]) -> None:
    print("\n" + "=" * 88)
    print("COMPARE REPORTS V2 (coach-grade, MAIN centré, sans matching strict)")
    print("=" * 88)
    print(f"A: {a_meta[0]} | {a_meta[1]} | {a_meta[2]}")
    print(f"B: {b_meta[0]} | {b_meta[1]} | {b_meta[2]}")
    print("-" * 88)

    ta_all = A["totals"]
    tb_all = B["totals"]

    # 1) MAIN first (this is the core comparison)
    print_block_summary("MAIN (prioritaire)", ta_all.get("MAIN", {}), tb_all.get("MAIN", {}),
                        A["density"].get("MAIN"), B["density"].get("MAIN"))

    # 2) Secondary blocks
    print_block_summary("WARMUP", ta_all.get("WARMUP", {}), tb_all.get("WARMUP", {}),
                        A["density"].get("WARMUP"), B["density"].get("WARMUP"))

    print_block_summary("COOLDOWN", ta_all.get("COOLDOWN", {}), tb_all.get("COOLDOWN", {}),
                        A["density"].get("COOLDOWN"), B["density"].get("COOLDOWN"))

    # 3) PAUSE/UNSPEC if present
    if (ta_all.get("PAUSE", {}).get("PAUSE_s", 0) > 0) or (tb_all.get("PAUSE", {}).get("PAUSE_s", 0) > 0):
        print_block_summary("PAUSE", ta_all.get("PAUSE", {}), tb_all.get("PAUSE", {}),
                            A["density"].get("PAUSE"), B["density"].get("PAUSE"))

    if (ta_all.get("UNSPEC", {}).get("WORK_s", 0) +
        ta_all.get("UNSPEC", {}).get("RECUP_s", 0) +
        ta_all.get("UNSPEC", {}).get("OTHER_s", 0) +
        ta_all.get("UNSPEC", {}).get("PAUSE_s", 0) > 0) or (
        tb_all.get("UNSPEC", {}).get("WORK_s", 0) +
        tb_all.get("UNSPEC", {}).get("RECUP_s", 0) +
        tb_all.get("UNSPEC", {}).get("OTHER_s", 0) +
        tb_all.get("UNSPEC", {}).get("PAUSE_s", 0) > 0
    ):
        print_block_summary("UNSPEC (tags sans block)", ta_all.get("UNSPEC", {}), tb_all.get("UNSPEC", {}),
                            A["density"].get("UNSPEC"), B["density"].get("UNSPEC"))

    # 4) MAIN tag details
    print("\n" + "-" * 88)
    print("DÉTAILS TAGS MAIN (comparaison factuelle)")
    print("-" * 88)

    keys = set(k for k in A["key_metrics"].keys() if k[0] == "MAIN") | set(k for k in B["key_metrics"].keys() if k[0] == "MAIN")

    def sort_key(k):
        block, tag = k
        ra = A["key_metrics"].get(k, {})
        rb = B["key_metrics"].get(k, {})
        sa = ra.get("total_s", 0) or 0
        sb = rb.get("total_s", 0) or 0
        # sort by total time desc, then tag
        return (-(sa + sb), tag)

    for (block, tag) in sorted(keys, key=sort_key):
        ra = A["key_metrics"].get((block, tag))
        rb = B["key_metrics"].get((block, tag))

        print(f"\n[MAIN] {tag}")

        if ra is None:
            print("  A: — (absent)")
        else:
            print(f"  A: {ra['n_laps']} laps | {ra['total_s']}s | {ra['total_m']:.0f}m | pace {fmt_pace(ra['pace'])} | HR {fmt_float(ra['hr'],1)} | pace_std {fmt_float(ra['pace_std'],1)} | HR_drift {fmt_float(ra['hr_drift'],1)}")

        if rb is None:
            print("  B: — (absent)")
        else:
            print(f"  B: {rb['n_laps']} laps | {rb['total_s']}s | {rb['total_m']:.0f}m | pace {fmt_pace(rb['pace'])} | HR {fmt_float(rb['hr'],1)} | pace_std {fmt_float(rb['pace_std'],1)} | HR_drift {fmt_float(rb['hr_drift'],1)}")

        if ra is not None and rb is not None:
            d_hr = (rb["hr"] - ra["hr"]) if (ra["hr"] is not None and rb["hr"] is not None) else None
            d_std = (rb["pace_std"] - ra["pace_std"]) if (ra["pace_std"] is not None and rb["pace_std"] is not None) else None
            print(f"  Δ: pace {fmt_delta_pace(ra['pace'], rb['pace'])} | HR {fmt_float(d_hr,1)} | pace_std {fmt_float(d_std,1)}")

    print("\n" + "=" * 88)
    print("")


def main():
    conn = sqlite3.connect(DB_PATH)

    if not ensure_lap_tags_table_exists(conn):
        print("Erreur: table lap_tags absente. Tagge au moins une séance avant de comparer.")
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

    a_id = int(input("\nactivity_id A = ").strip())
    b_id = int(input("activity_id B = ").strip())

    a_meta = fetch_activity_meta(conn, a_id)
    b_meta = fetch_activity_meta(conn, b_id)
    if not a_meta or not b_meta:
        print("Erreur: activity_id invalide.")
        return

    A = compute_report_metrics(conn, a_id)
    B = compute_report_metrics(conn, b_id)

    print_compare(a_meta, b_meta, A, B)
    conn.close()


if __name__ == "__main__":
    main()
