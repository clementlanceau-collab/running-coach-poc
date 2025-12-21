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


def pace_from_time_distance(elapsed_s: int, dist_m: float) -> Optional[float]:
    if dist_m is None or dist_m <= 0:
        return None
    return (elapsed_s / dist_m) * 1000.0


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


def classify_role(tag: Optional[str]) -> str:
    t = (tag or "").lower()
    if "recup" in t:
        return "RECUP"
    if t.startswith("set_") or t.startswith("strides_"):
        return "WORK"
    # fallback: if user tags something like "tempo" we might still treat as WORK later
    return "OTHER"


def fetch_activity_meta(conn: sqlite3.Connection, activity_id: int) -> Optional[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type, device_name
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


def compute_generic(conn: sqlite3.Connection, activity_id: int) -> Dict[str, Any]:
    meta = fetch_activity_meta(conn, activity_id)
    if not meta:
        raise RuntimeError("activity_id introuvable")

    laps = fetch_tagged_laps(conn, activity_id)
    if not laps:
        return {"activity_id": activity_id, "meta": meta, "note": "NO_TAGS"}

    # per tag metrics
    by_tag: Dict[Tuple[str, str], List[Tuple]] = {}
    for (tag, block, lap_index, elapsed_s, dist_m, s_idx, e_idx) in laps:
        by_tag.setdefault((tag, block), []).append((lap_index, elapsed_s, dist_m, s_idx, e_idx))

    tag_rows = []
    totals = {
        "WORK": {"s": 0, "m": 0.0, "hrs": [], "paces": []},
        "RECUP": {"s": 0, "m": 0.0, "hrs": [], "paces": []},
        "OTHER": {"s": 0, "m": 0.0, "hrs": [], "paces": []},
    }

    for (tag, block), rows in by_tag.items():
        total_s = 0
        total_m = 0.0
        lap_paces = []
        lap_hrs = []

        for (_, elapsed_s, dist_m, s_idx, e_idx) in rows:
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

        role = classify_role(tag)
        pace_mean = pace_from_time_distance(total_s, total_m) if total_m > 0 else None
        hr_mean = mean(lap_hrs)
        pace_std = std(lap_paces)

        tag_rows.append({
            "tag": tag,
            "block": block,
            "role": role,
            "n_laps": len(rows),
            "total_s": total_s,
            "total_m": total_m,
            "pace": pace_mean,
            "hr": hr_mean,
            "pace_std": pace_std
        })

        # aggregate to role totals
        totals[role]["s"] += total_s
        totals[role]["m"] += total_m
        if hr_mean is not None:
            totals[role]["hrs"].append(hr_mean)
        if pace_mean is not None:
            totals[role]["paces"].append(pace_mean)

    # density (work/(work+recup))
    density = None
    if totals["WORK"]["s"] + totals["RECUP"]["s"] > 0:
        density = totals["WORK"]["s"] / (totals["WORK"]["s"] + totals["RECUP"]["s"])

    # global-ish pace/hr for WORK (average of tag paces/hrs)
    work_pace = mean(totals["WORK"]["paces"])
    work_hr = mean(totals["WORK"]["hrs"])
    work_pace_std_across_tags = std(totals["WORK"]["paces"])

    return {
        "activity_id": activity_id,
        "meta": meta,
        "tag_rows": tag_rows,
        "totals": totals,
        "density": density,
        "work_pace": work_pace,
        "work_hr": work_hr,
        "work_pace_std_across_tags": work_pace_std_across_tags
    }


def print_generic_compare(A: Dict[str, Any], B: Dict[str, Any]) -> None:
    a_meta = A["meta"]
    b_meta = B["meta"]

    print("\n" + "=" * 72)
    print("GENERIC COMPARE (séance vs séance, structure libre)")
    print("=" * 72)
    print(f"A: {a_meta[0]} | {a_meta[1]} | {a_meta[2]} | {a_meta[3]} | {a_meta[4]}")
    print(f"B: {b_meta[0]} | {b_meta[1]} | {b_meta[2]} | {b_meta[3]} | {b_meta[4]}")
    print("-" * 72)

    def line(role: str):
        a = A["totals"][role]
        b = B["totals"][role]
        print(f"{role:<5}  A: {a['s']/60.0:>6.1f} min | {a['m']/1000.0:>6.3f} km   ||   "
              f"B: {b['s']/60.0:>6.1f} min | {b['m']/1000.0:>6.3f} km")

    line("WORK")
    line("RECUP")
    line("OTHER")

    da = A["density"]
    db = B["density"]
    print(f"Densité work/(w+r)  A: {da*100.0:.1f}%   ||   B: {db*100.0:.1f}%"
          if (da is not None and db is not None) else "Densité work/(w+r) : —")

    print(f"WORK pace (moy tags) A: {fmt_pace(A['work_pace'])}   ||   B: {fmt_pace(B['work_pace'])}")
    print(f"WORK HR  (moy tags) A: {A['work_hr']:.1f}" + (f"   ||   B: {B['work_hr']:.1f}" if B["work_hr"] is not None else "   ||   B: —")
          if A["work_hr"] is not None else "WORK HR  (moy tags) : —")
    print(f"WORK variabilité (across tags, pace std) A: {fmt_float(A['work_pace_std_across_tags'],1)}   ||   B: {fmt_float(B['work_pace_std_across_tags'],1)}")

    # Top tags WORK
    def top_work(session: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = [r for r in session["tag_rows"] if r["role"] == "WORK"]
        rows.sort(key=lambda r: -r["total_s"])
        return rows[:5]

    print("\nTop WORK tags (par durée):")
    print("A:")
    for r in top_work(A):
        print(f"  {r['tag']} ({r['block']}): {r['total_s']}s | {r['total_m']:.0f}m | pace {fmt_pace(r['pace'])} | HR {fmt_float(r['hr'],1)} | pace_std {fmt_float(r['pace_std'],1)}")
    print("B:")
    for r in top_work(B):
        print(f"  {r['tag']} ({r['block']}): {r['total_s']}s | {r['total_m']:.0f}m | pace {fmt_pace(r['pace'])} | HR {fmt_float(r['hr'],1)} | pace_std {fmt_float(r['pace_std'],1)}")

    print("\nNOTE: comparaison robuste (niveau blocs). Pour piste vs route, interpréter pace avec prudence.")
    print("=" * 72)
    print("")


def fmt_float(x: Optional[float], nd: int = 1) -> str:
    if x is None:
        return "—"
    return f"{x:.{nd}f}"


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

    A = compute_generic(conn, a_id)
    B = compute_generic(conn, b_id)

    if A.get("note") == "NO_TAGS" or B.get("note") == "NO_TAGS":
        print("\nErreur: une des deux séances n’a pas de tags (lap_tags).")
        print("-> Tagge-la d’abord via: python -m app.analysis.tag_laps")
        conn.close()
        return

    print_generic_compare(A, B)
    conn.close()


if __name__ == "__main__":
    main()
