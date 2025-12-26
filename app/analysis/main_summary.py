import sqlite3
from typing import List, Tuple, Optional, Dict, Any
import math

DB_PATH = "running.db"


# ----------------------------
# Format helpers
# ----------------------------
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


def fmt_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    s = int(round(seconds))
    mm = s // 60
    ss = s % 60
    return f"{mm}:{ss:02d}"


def fmt_distance_m(m: Optional[float]) -> str:
    if m is None:
        return "—"
    return f"{m/1000.0:.3f} km"


def pace_from_time_distance(elapsed_s: float, dist_m: float) -> Optional[float]:
    if dist_m is None or dist_m <= 0:
        return None
    return (elapsed_s / dist_m) * 1000.0


# ----------------------------
# Stats helpers
# ----------------------------
def mean(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def median(values: List[float]) -> Optional[float]:
    vals = sorted([v for v in values if v is not None])
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    if n % 2 == 1:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0


def std(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        if len(vals) == 1:
            return 0.0
        return None
    m = sum(vals) / len(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (len(vals) - 1))


def weighted_mean(values: List[float], weights: List[float]) -> Optional[float]:
    pairs = [(v, w) for v, w in zip(values, weights) if v is not None and w is not None and w > 0]
    if not pairs:
        return None
    num = sum(v * w for v, w in pairs)
    den = sum(w for _, w in pairs)
    if den <= 0:
        return None
    return num / den


def weighted_std(values: List[float], weights: List[float]) -> Optional[float]:
    pairs = [(v, w) for v, w in zip(values, weights) if v is not None and w is not None and w > 0]
    if len(pairs) < 2:
        if len(pairs) == 1:
            return 0.0
        return None
    m = weighted_mean([v for v, _ in pairs], [w for _, w in pairs])
    if m is None:
        return None
    num = sum(w * ((v - m) ** 2) for v, w in pairs)
    den = sum(w for _, w in pairs)
    if den <= 0:
        return None
    return math.sqrt(num / den)


# ----------------------------
# Domain logic
# ----------------------------
def classify_role(tag: str) -> str:
    t = (tag or "").lower()

    # Pause tags (tu peux enrichir si besoin)
    if "pause" in t or "stop" in t:
        return "PAUSE"

    if "recup" in t:
        return "RECUP"
    if t.startswith("set_") or t.startswith("strides_"):
        return "WORK"
    return "OTHER"


def work_bucket(rep_s: Optional[float]) -> str:
    """
    Bucket basé sur la durée typique de répétition (médiane des laps dans un tag WORK).
    """
    if rep_s is None or rep_s <= 0:
        return "UNK"
    if rep_s <= 45:
        return "SHORT"
    if rep_s <= 150:
        return "MID"
    return "LONG"


# ----------------------------
# DB fetch
# ----------------------------
def fetch_activity_meta(conn: sqlite3.Connection, activity_id: int) -> Optional[Tuple]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT activity_id, start_date_local, name, sport_type, device_name, has_heartrate
        FROM activities
        WHERE activity_id = ?;
        """,
        (activity_id,),
    )
    return cur.fetchone()


def fetch_tagged_laps_main(conn: sqlite3.Connection, activity_id: int) -> List[Tuple]:
    """
    Retourne les laps taggés "MAIN" (source STRAVA_LAP).

    columns:
      tag, block, lap_index, elapsed_s, dist_m, start_index, end_index
    """
    cur = conn.cursor()
    cur.execute(
        """
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
          AND t.source='STRAVA_LAP'
          AND COALESCE(t.block,'UNSPEC') = 'MAIN'
        ORDER BY t.tag, t.lap_index;
        """,
        (activity_id,),
    )
    return cur.fetchall()


def hr_avg_from_stream(conn: sqlite3.Connection, activity_id: int, start_idx: Optional[int], end_idx: Optional[int]) -> Optional[float]:
    if start_idx is None or end_idx is None:
        return None
    cur = conn.cursor()
    cur.execute(
        """
        SELECT AVG(heartrate_bpm)
        FROM stream_points
        WHERE activity_id = ?
          AND idx BETWEEN ? AND ?
          AND heartrate_bpm IS NOT NULL;
        """,
        (activity_id, int(start_idx), int(end_idx)),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return float(row[0])


# ----------------------------
# Compute report
# ----------------------------
def compute_main_summary(conn: sqlite3.Connection, activity_id: int) -> Dict[str, Any]:
    laps = fetch_tagged_laps_main(conn, activity_id)

    # group by tag
    by_tag: Dict[str, List[Tuple]] = {}
    for (tag, block, lap_index, elapsed_s, dist_m, s_idx, e_idx) in laps:
        by_tag.setdefault(tag, []).append((lap_index, elapsed_s, dist_m, s_idx, e_idx))

    tag_metrics: Dict[str, Dict[str, Any]] = {}

    # Totaux MAIN par rôle
    totals = {
        "WORK": {"s": 0.0, "m": 0.0},
        "RECUP": {"s": 0.0, "m": 0.0},
        "OTHER": {"s": 0.0, "m": 0.0},
        "PAUSE": {"s": 0.0, "m": 0.0},
    }

    # Build per-tag metrics
    for tag, rows in by_tag.items():
        role = classify_role(tag)

        total_s = 0.0
        total_m = 0.0

        lap_paces: List[float] = []
        lap_hrs: List[float] = []
        lap_durs: List[float] = []

        for (lap_index, elapsed_s, dist_m, s_idx, e_idx) in rows:
            if elapsed_s is None or dist_m is None:
                continue

            elapsed_s = float(elapsed_s)
            dist_m = float(dist_m)

            total_s += elapsed_s
            total_m += dist_m
            lap_durs.append(elapsed_s)

            p = pace_from_time_distance(elapsed_s, dist_m)
            if p is not None:
                lap_paces.append(p)

            hr = hr_avg_from_stream(conn, activity_id, s_idx, e_idx)
            if hr is not None:
                lap_hrs.append(hr)

        pace = pace_from_time_distance(total_s, total_m) if total_m > 0 else None
        hr = mean(lap_hrs)

        pace_std = std(lap_paces)
        hr_drift = None
        if len(lap_hrs) >= 2:
            hr_drift = lap_hrs[-1] - lap_hrs[0]

        rep_s = median(lap_durs)  # durée typique par répétition (lap)
        bucket = None
        if role == "WORK":
            bucket = work_bucket(rep_s)

        totals[role]["s"] += total_s
        totals[role]["m"] += total_m

        tag_metrics[tag] = {
            "tag": tag,
            "role": role,
            "n_laps": len(rows),
            "total_s": total_s,
            "total_m": total_m,
            "pace": pace,
            "hr": hr,
            "pace_std": pace_std,
            "hr_drift": hr_drift,
            "rep_s": rep_s,
            "bucket": bucket,
        }

    # MAIN density
    density = None
    work_s = totals["WORK"]["s"]
    rec_s = totals["RECUP"]["s"]
    if (work_s + rec_s) > 0:
        density = work_s / (work_s + rec_s)

    # WORK indicators weighted by tag time
    work_tags = [m for m in tag_metrics.values() if m["role"] == "WORK" and m["total_s"] > 0]
    recup_tags = [m for m in tag_metrics.values() if m["role"] == "RECUP" and m["total_s"] > 0]

    def weighted_over(tags_list: List[Dict[str, Any]], field: str) -> Optional[float]:
        vals = [t.get(field) for t in tags_list]
        w = [t.get("total_s") for t in tags_list]
        return weighted_mean(vals, w)

    def weighted_std_over(tags_list: List[Dict[str, Any]], field: str) -> Optional[float]:
        vals = [t.get(field) for t in tags_list]
        w = [t.get("total_s") for t in tags_list]
        return weighted_std(vals, w)

    work_pace_w = weighted_over(work_tags, "pace")
    work_hr_w = weighted_over(work_tags, "hr")
    work_pace_std_w = weighted_over(work_tags, "pace_std")
    work_hr_drift_w = weighted_over(work_tags, "hr_drift")
    work_inter_tag_pace_std = weighted_std_over(work_tags, "pace")

    rec_pace_w = weighted_over(recup_tags, "pace")
    rec_hr_w = weighted_over(recup_tags, "hr")

    # Buckets summary
    buckets = ["SHORT", "MID", "LONG", "UNK"]
    bucket_summary: Dict[str, Dict[str, Any]] = {}
    for b in buckets:
        btags = [t for t in work_tags if t.get("bucket") == b]
        b_work_s = sum(t["total_s"] for t in btags)
        b_work_m = sum(t["total_m"] for t in btags)
        pct = (b_work_s / work_s) if work_s > 0 else 0.0

        bucket_summary[b] = {
    "bucket": b,
    "n_tags": len(btags),
    "total_s": b_work_s,
    "total_m": b_work_m,
    "pct": pct,
    "pace_w": weighted_over(btags, "pace"),
    "hr_w": weighted_over(btags, "hr"),
    "pace_std_w": weighted_over(btags, "pace_std"),
    "hr_drift_w": weighted_over(btags, "hr_drift"),
    "inter_tag_pace_std": (weighted_std_over(btags, "pace") if len(btags) >= 2 else None),
}


    return {
        "activity_id": activity_id,
        "tag_metrics": tag_metrics,
        "totals": totals,
        "density": density,
        "work_indicators": {
            "pace_w": work_pace_w,
            "hr_w": work_hr_w,
            "pace_std_w": work_pace_std_w,
            "hr_drift_w": work_hr_drift_w,
            "inter_tag_pace_std": work_inter_tag_pace_std,
        },
        "recup_indicators": {
            "pace_w": rec_pace_w,
            "hr_w": rec_hr_w,
        },
        "bucket_summary": bucket_summary,
    }


# ----------------------------
# Print report
# ----------------------------
def print_main_summary(meta: Tuple, summary: Dict[str, Any]) -> None:
    activity_id, start_date_local, name, sport_type, device_name, has_hr = meta

    line = "=" * 96
    print("\n" + line)
    print("MAIN SUMMARY (coach-grade: WORK structuré par durée, pondéré par le temps)")
    print(line)

    print(f"activity_id : {activity_id}")
    print(f"date        : {start_date_local}")
    print(f"name        : {name}")
    print(f"sport_type  : {sport_type}")
    print(f"device      : {device_name}")
    print(f"has_hr      : {has_hr}")
    print("-" * 96)

    totals = summary["totals"]
    density = summary["density"]

    # MAIN volumes (sans PAUSE en densité, mais on l'affiche quand même)
    work_s = totals["WORK"]["s"]
    work_m = totals["WORK"]["m"]
    rec_s = totals["RECUP"]["s"]
    rec_m = totals["RECUP"]["m"]
    other_s = totals["OTHER"]["s"]
    other_m = totals["OTHER"]["m"]
    pause_s = totals["PAUSE"]["s"]
    pause_m = totals["PAUSE"]["m"]

    print("[MAIN volumes]")
    print(f"  WORK  : {fmt_duration(work_s)} | {fmt_distance_m(work_m)}")
    print(f"  RECUP : {fmt_duration(rec_s)} | {fmt_distance_m(rec_m)}")
    print(f"  OTHER : {fmt_duration(other_s)} | {fmt_distance_m(other_m)}")
    if pause_s > 0:
        print(f"  PAUSE : {fmt_duration(pause_s)} | {fmt_distance_m(pause_m)}")
    print(f"  Densité (WORK/(WORK+RECUP)) : {fmt_float(density * 100.0 if density is not None else None, 1)}%")
    print("")

    # MAIN indicators (WORK)
    wi = summary["work_indicators"]
    print("[MAIN indicators (WORK, agrégé)]")
    print("  (pondérés par le temps WORK de chaque tag)")
    print(f"  pace WORK moyen (pondéré)              : {fmt_pace(wi['pace_w'])}")
    print(f"  HR WORK moyen (pondéré)                : {fmt_float(wi['hr_w'], 1)}")
    print(f"  stabilité pace intra-tag (std, pond.)  : {fmt_float(wi['pace_std_w'], 1)} s/km")
    print(f"  dérive HR intra-tag (pondérée)         : {fmt_float(wi['hr_drift_w'], 1)} bpm")
    print(f"  variabilité inter-tags (pace std)      : {fmt_float(wi['inter_tag_pace_std'], 1)} s/km")
    print("")

    # MAIN recovery (RECUP vs WORK)
    ri = summary["recup_indicators"]
    if ri["pace_w"] is not None and wi["pace_w"] is not None:
        delta_pace = ri["pace_w"] - wi["pace_w"]
    else:
        delta_pace = None

    if ri["hr_w"] is not None and wi["hr_w"] is not None:
        delta_hr = ri["hr_w"] - wi["hr_w"]
    else:
        delta_hr = None

    print("[MAIN recovery (RECUP vs WORK)]")
    print(f"  pace RECUP moyen (pondéré)             : {fmt_pace(ri['pace_w'])}")
    print(f"  HR RECUP moyen (pondéré)               : {fmt_float(ri['hr_w'], 1)}")
    print(f"  écart pace (RECUP - WORK)              : {fmt_float(delta_pace, 1)} s/km")
    print(f"  écart HR   (RECUP - WORK)              : {fmt_float(delta_hr, 1)} bpm")
    print("")

    # WORK composition (par bucket)
    bs = summary["bucket_summary"]
    print("[WORK composition (par durée typique de répétition)]")
    print("  Buckets: SHORT<=45s | MID=46-150s | LONG>=151s (pondérés par temps WORK)")
    for b in ["LONG", "MID", "SHORT", "UNK"]:
        row = bs[b]
        print(
            f"  - {b:<5}: {fmt_duration(row['total_s'])} | {fmt_float(row['pct']*100.0, 1)}% "
            f"| pace {fmt_pace(row['pace_w'])} | HR {fmt_float(row['hr_w'], 1)}"
        )
        # Les métriques qui rendent la comparaison “coach-grade”
        print(
            f"           intra-tag pace_std(w) {fmt_float(row['pace_std_w'],1)} s/km"
            f" | intra-tag HR_drift(w) {fmt_float(row['hr_drift_w'],1)} bpm"
            f" | inter-tag pace std {fmt_float(row['inter_tag_pace_std'],1)} s/km"
        )
    print("")

    # MAIN details par tag
    print("[MAIN details par tag]\n")
    # tri : WORK d'abord, puis RECUP, puis OTHER/PAUSE ; et par durée totale desc
    role_rank = {"WORK": 1, "RECUP": 2, "OTHER": 3, "PAUSE": 4}
    tags_sorted = sorted(
        summary["tag_metrics"].values(),
        key=lambda x: (role_rank.get(x["role"], 9), -(x["total_s"] or 0), x["tag"]),
    )

    for m in tags_sorted:
        tag = m["tag"]
        role = m["role"]
        if m["total_s"] <= 0:
            continue

        extra = ""
        if role == "WORK":
            rep = m.get("rep_s")
            b = m.get("bucket")
            extra = f" | rep~{int(round(rep)) if rep is not None else '—'}s | bucket={b}"

        print(f"  - {tag} [{role}]")
        print(
            f"      {m['n_laps']} laps | {int(round(m['total_s']))}s | {m['total_m']:.0f}m "
            f"| pace {fmt_pace(m['pace'])} | HR {fmt_float(m['hr'],1)} "
            f"| pace_std {fmt_float(m['pace_std'],1)} | HR_drift {fmt_float(m['hr_drift'],1)}{extra}"
        )
        print("")

    print(line)
    print("")


# ----------------------------
# CLI entrypoint
# ----------------------------
def main():
    conn = sqlite3.connect(DB_PATH)

    print("\nDernières activités RUN/TRAIL (streams OK):")
    cur = conn.cursor()
    cur.execute(
        """
        SELECT activity_id, start_date_local, name
        FROM activities
        WHERE sport_type IN ('Run','Trail Run')
          AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT 20;
        """
    )
    rows = cur.fetchall()
    for r in rows:
        print(f"{r[0]} | {r[1]} | {r[2]}")

    raw = input("\nactivity_id = ").strip()
    if not raw:
        print("Erreur: activity_id requis.")
        conn.close()
        return

    activity_id = int(raw)

    meta = fetch_activity_meta(conn, activity_id)
    if not meta:
        print("Erreur: activity_id invalide.")
        conn.close()
        return

    summary = compute_main_summary(conn, activity_id)
    print_main_summary(meta, summary)

    conn.close()


if __name__ == "__main__":
    main()
