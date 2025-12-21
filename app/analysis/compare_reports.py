import sqlite3
from typing import List, Tuple, Optional, Dict, Any
import math

DB_PATH = "running.db"


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


def mean(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def pace_from_time_distance(elapsed_s: int, dist_m: float) -> Optional[float]:
    if dist_m is None or dist_m <= 0:
        return None
    return (elapsed_s / dist_m) * 1000.0


def classify_role(tag: str) -> str:
    t = (tag or "").lower()
    if "recup" in t:
        return "RECUP"
    if t.startswith("set_") or t.startswith("strides_"):
        return "WORK"
    return "OTHER"


def fetch_activity_meta(conn: sqlite3.Connection, activity_id: int) -> Optional[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type
        FROM activities
        WHERE activity_id = ?;
    """, (activity_id,))
    return cur.fetchone()


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
        ORDER BY t.tag, t.lap_index;
    """, (activity_id,))
    return cur.fetchall()


def hr_avg_from_stream(conn: sqlite3.Connection, activity_id: int, start_idx: Optional[int], end_idx: Optional[int]) -> Optional[float]:
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


def compute_report_metrics(conn: sqlite3.Connection, activity_id: int) -> Dict[str, Any]:
    laps = fetch_tagged_laps(conn, activity_id)
    by_tag: Dict[Tuple[str, str], List[Tuple]] = {}
    for (tag, block, lap_index, elapsed_s, dist_m, s_idx, e_idx) in laps:
        by_tag.setdefault((tag, block), []).append((lap_index, elapsed_s, dist_m, s_idx, e_idx))

    # per tag metrics
    tag_metrics: Dict[Tuple[str, str], Dict[str, Any]] = {}

    total_work_s = total_work_m = 0.0
    total_rec_s = total_rec_m = 0.0
    total_other_s = total_other_m = 0.0

    for (tag, block), rows in by_tag.items():
        total_s = 0
        total_m = 0.0
        lap_paces = []
        lap_hrs = []

        for (lap_index, elapsed_s, dist_m, s_idx, e_idx) in rows:
            if elapsed_s is None or dist_m is None:
                continue
            elapsed_s = int(elapsed_s)
            dist_m = float(dist_m)

            total_s += elapsed_s
            total_m += dist_m

            pace = pace_from_time_distance(elapsed_s, dist_m)
            if pace is not None:
                lap_paces.append(pace)

            hr = hr_avg_from_stream(conn, activity_id, s_idx, e_idx)
            if hr is not None:
                lap_hrs.append(hr)

        pace = pace_from_time_distance(total_s, total_m) if total_m > 0 else None
        hr = mean(lap_hrs)

        # simple variability across laps in tag (pace std)
        pace_std = None
        if len(lap_paces) >= 2:
            m = sum(lap_paces) / len(lap_paces)
            pace_std = math.sqrt(sum((x - m) ** 2 for x in lap_paces) / (len(lap_paces) - 1))
        elif len(lap_paces) == 1:
            pace_std = 0.0

        # HR drift (first/last lap in that tag) if HR available for those laps
        hr_drift = None
        if len(lap_hrs) >= 2:
            hr_drift = lap_hrs[-1] - lap_hrs[0]

        role = classify_role(tag)
        if role == "WORK":
            total_work_s += total_s
            total_work_m += total_m
        elif role == "RECUP":
            total_rec_s += total_s
            total_rec_m += total_m
        else:
            total_other_s += total_s
            total_other_m += total_m

        tag_metrics[(tag, block)] = {
            "tag": tag,
            "block": block,
            "role": role,
            "n_laps": len(rows),
            "total_s": total_s,
            "total_m": total_m,
            "pace": pace,
            "hr": hr,
            "pace_std": pace_std,
            "hr_drift": hr_drift
        }

    density = None
    if (total_work_s + total_rec_s) > 0:
        density = total_work_s / (total_work_s + total_rec_s)

    return {
        "activity_id": activity_id,
        "tag_metrics": tag_metrics,
        "totals": {
            "work_s": total_work_s, "work_m": total_work_m,
            "rec_s": total_rec_s, "rec_m": total_rec_m,
            "other_s": total_other_s, "other_m": total_other_m,
            "density": density
        }
    }


def print_compare(a_meta: Tuple, b_meta: Tuple, A: Dict[str, Any], B: Dict[str, Any]) -> None:
    print("\n" + "=" * 72)
    print("COMPARE REPORTS (structuré, sans matching strict)")
    print("=" * 72)
    print(f"A: {a_meta[0]} | {a_meta[1]} | {a_meta[2]}")
    print(f"B: {b_meta[0]} | {b_meta[1]} | {b_meta[2]}")
    print("-" * 72)

    ta = A["totals"]
    tb = B["totals"]

    def line_tot(label: str, key_s: str, key_m: str):
        print(f"{label:<14} A: {ta[key_s]/60.0:>6.1f} min | {ta[key_m]/1000.0:>6.3f} km"
              f"   ||   B: {tb[key_s]/60.0:>6.1f} min | {tb[key_m]/1000.0:>6.3f} km")

    line_tot("WORK", "work_s", "work_m")
    line_tot("RECUP", "rec_s", "rec_m")
    line_tot("OTHER", "other_s", "other_m")

    da = ta["density"]
    db = tb["density"]
    print(f"Densité(work/(w+r)) A: {fmt_float(da*100.0 if da is not None else None, 1)}%   ||   "
          f"B: {fmt_float(db*100.0 if db is not None else None, 1)}%")

    print("\n--- Détails par tag (MAIN d’abord) ---")

    # union tags
    keys = set(A["tag_metrics"].keys()) | set(B["tag_metrics"].keys())

    # sort: block order then role then total_s desc (from A then B)
    block_rank = {"WARMUP": 1, "MAIN": 2, "COOLDOWN": 3, "UNSPEC": 9}

    def sort_key(k):
        tag, block = k
        ra = A["tag_metrics"].get(k, {})
        rb = B["tag_metrics"].get(k, {})
        sa = ra.get("total_s", 0) or 0
        sb = rb.get("total_s", 0) or 0
        return (block_rank.get(block, 9), tag, -(sa + sb))

    for (tag, block) in sorted(keys, key=sort_key):
        ra = A["tag_metrics"].get((tag, block))
        rb = B["tag_metrics"].get((tag, block))

        # Focus MAIN first, but still print all
        print(f"\n[{block}] {tag}")

        if ra is None:
            print("  A: — (absent)")
        else:
            print(f"  A: {ra['n_laps']} laps | {ra['total_s']}s | {ra['total_m']:.0f}m | pace {fmt_pace(ra['pace'])} | HR {fmt_float(ra['hr'],1)} | pace_std {fmt_float(ra['pace_std'],1)} | HR_drift {fmt_float(ra['hr_drift'],1)}")

        if rb is None:
            print("  B: — (absent)")
        else:
            print(f"  B: {rb['n_laps']} laps | {rb['total_s']}s | {rb['total_m']:.0f}m | pace {fmt_pace(rb['pace'])} | HR {fmt_float(rb['hr'],1)} | pace_std {fmt_float(rb['pace_std'],1)} | HR_drift {fmt_float(rb['hr_drift'],1)}")

        # deltas if both present
        if ra is not None and rb is not None:
            print(f"  Δ: pace {fmt_delta_pace(ra['pace'], rb['pace'])} | HR {fmt_float((rb['hr']-ra['hr']) if (ra['hr'] is not None and rb['hr'] is not None) else None,1)} | pace_std {fmt_float((rb['pace_std']-ra['pace_std']) if (ra['pace_std'] is not None and rb['pace_std'] is not None) else None,1)}")

    print("\n" + "=" * 72)
    print("")


def main():
    conn = sqlite3.connect(DB_PATH)

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
