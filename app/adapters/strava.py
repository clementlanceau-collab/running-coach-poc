from dotenv import load_dotenv
load_dotenv()

import os
import time
import sqlite3
import requests
from typing import Dict, Any, Optional, List, Tuple


STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"


# -----------------------
# OAuth + HTTP
# -----------------------
def refresh_access_token() -> str:
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")

    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError(
            "Variables manquantes: STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN"
        )

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
        r = requests.get(url, headers=headers, params=params, timeout=20)
    except requests.exceptions.RequestException:
        time.sleep(2)
        r = requests.get(url, headers=headers, params=params, timeout=20)

    if r.status_code == 401:
        try:
            payload = r.json()
        except Exception:
            payload = None

        if isinstance(payload, dict) and payload.get("message") == "Authorization Error":
            refresh_access_token()
            token = os.getenv("STRAVA_ACCESS_TOKEN")
            headers = {"Authorization": f"Bearer {token}"}
            r = requests.get(url, headers=headers, params=params, timeout=20)

    return r


# -----------------------
# SQLite schema + helpers
# -----------------------
def init_db(db_path: str = "running.db") -> None:
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
    CREATE TABLE IF NOT EXISTS laps_auto (
        activity_id INTEGER NOT NULL,
        lap_index INTEGER NOT NULL,
        lap_type TEXT NOT NULL,          -- 'KM', 'WARMUP', 'EFFORT', 'RECUP', 'COOLDOWN'
        start_idx INTEGER NOT NULL,
        end_idx INTEGER NOT NULL,
        start_time_s INTEGER,
        end_time_s INTEGER,
        duration_s INTEGER,
        distance_m REAL,
        pace_s_per_km REAL,
        avg_hr REAL,
        avg_grade REAL,
        PRIMARY KEY (activity_id, lap_type, lap_index)
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_stream_points_act_idx ON stream_points(activity_id, idx);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_laps_auto_act_type ON laps_auto(activity_id, lap_type, lap_index);")

    conn.commit()
    conn.close()


def _fetch_stream_points(activity_id: int, db_path: str) -> List[Tuple]:
    """
    Retour:
      (idx, time_s, distance_m, velocity_m_s, heartrate_bpm, grade)
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT idx, time_s, distance_m, velocity_m_s, heartrate_bpm, grade
        FROM stream_points
        WHERE activity_id = ?
        ORDER BY idx ASC;
    """, (activity_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def _mean(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _median(values: List[float]) -> Optional[float]:
    vals = sorted([v for v in values if v is not None])
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    if n % 2 == 1:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2


def _pace_from_duration_distance(duration_s: int, dist_m: float) -> Optional[float]:
    if dist_m <= 0:
        return None
    return (duration_s / dist_m) * 1000.0


def _rolling_mean(values: List[Optional[float]], window: int) -> List[Optional[float]]:
    if window <= 1:
        return values[:]
    n = len(values)
    half = window // 2
    out: List[Optional[float]] = [None] * n
    for i in range(n):
        s = max(0, i - half)
        e = min(n, i + half + 1)
        chunk = [v for v in values[s:e] if isinstance(v, (int, float))]
        out[i] = (sum(chunk) / len(chunk)) if chunk else None
    return out


def _median_mad(values: List[float]) -> Tuple[float, float]:
    vals = sorted(values)
    med = _median(vals)
    if med is None:
        return 0.0, 0.0
    abs_dev = [abs(v - med) for v in vals]
    mad = _median(abs_dev) or 0.0
    return float(med), float(mad)


def _robust_z(v: Optional[float], med: float, mad: float) -> Optional[float]:
    if v is None:
        return None
    scale = 1.4826 * mad
    if scale <= 1e-9:
        return 0.0
    return (float(v) - med) / scale


# -----------------------
# Listing activités (pour choisir une séance qualité)
# -----------------------
def list_recent_runs_with_streams(db_path: str = "running.db", limit: int = 20) -> List[Tuple]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type
        FROM activities
        WHERE sport_type IN ('Run', 'Trail Run') AND streams_status = 'OK'
        ORDER BY start_date_local DESC
        LIMIT ?;
    """, (limit,))
    acts = cur.fetchall()

    out = []
    for (activity_id, start_date_local, name, sport_type) in acts:
        cur.execute("""
            SELECT time_s, distance_m
            FROM stream_points
            WHERE activity_id = ?
            ORDER BY idx DESC
            LIMIT 1;
        """, (activity_id,))
        last = cur.fetchone()
        if not last:
            continue
        time_s, distance_m = last
        duration_min = (time_s / 60.0) if time_s is not None else None
        distance_km = (distance_m / 1000.0) if distance_m is not None else None
        out.append((activity_id, start_date_local, name, sport_type, duration_min, distance_km))

    conn.close()
    return out


def print_recent_runs_with_streams(db_path: str = "running.db", limit: int = 20) -> None:
    rows = list_recent_runs_with_streams(db_path=db_path, limit=limit)
    print("\nDernières activités RUN/TRAIL avec streams (choisis une séance 'qualité'):")
    print("activity_id | date | distance_km | durée_min | sport_type | name")
    for (aid, dt, name, st, dur_min, dist_km) in rows:
        dist_txt = f"{dist_km:.2f}" if isinstance(dist_km, (int, float)) else "?"
        dur_txt = f"{dur_min:.1f}" if isinstance(dur_min, (int, float)) else "?"
        print(f"{aid} | {dt} | {dist_txt} | {dur_txt} | {st} | {name}")
    print("")


# -----------------------
# V4: Détection structure time-based
#   - EFFORT robustes
#   - RECUP = gaps entre efforts
#   - WARMUP / COOLDOWN = avant/après
#   - clustering par durée des efforts (console)
# -----------------------
def _make_row(activity_id: int,
              lap_type: str,
              lap_index: int,
              pts: List[Tuple],
              s_i: int,
              e_i: int) -> Tuple:
    # pts: (idx, time_s, distance_m, velocity_m_s, heartrate_bpm, grade)
    idx0, t0, d0, _, _, _ = pts[s_i]
    idx1, t1, d1, _, _, _ = pts[e_i]

    if t0 is None or t1 is None:
        duration = int(max(0, e_i - s_i))
    else:
        duration = int(max(0, t1 - t0))

    if d0 is None or d1 is None:
        dist_seg = None
    else:
        dist_seg = float(max(0.0, d1 - d0))

    pace = _pace_from_duration_distance(duration, dist_seg) if dist_seg is not None else None

    hr_seg = [p[4] for p in pts[s_i:e_i + 1] if p[4] is not None]
    gr_seg = [p[5] for p in pts[s_i:e_i + 1] if p[5] is not None]
    avg_hr = _mean([float(x) for x in hr_seg]) if hr_seg else None
    avg_grade = _mean([float(x) for x in gr_seg]) if gr_seg else None

    return (
        activity_id,
        lap_index,
        lap_type,
        int(idx0),
        int(idx1),
        int(t0) if t0 is not None else None,
        int(t1) if t1 is not None else None,
        duration,
        dist_seg,
        pace,
        avg_hr,
        avg_grade,
    )


def _cluster_efforts_by_duration(durations: List[int]) -> List[Dict[str, Any]]:
    """
    Clustering simple et robuste pour des durées comme 180s, 90s, 30s.
    On groupe si proche de +/- max(8s, 15%).
    Retour: liste de clusters avec median_dur et membres.
    """
    if not durations:
        return []

    remaining = durations[:]
    clusters: List[Dict[str, Any]] = []

    # seed par plus longues d'abord (pratique sur séances multi-blocs)
    remaining.sort(reverse=True)

    while remaining:
        seed = remaining[0]
        tol = max(8, int(round(seed * 0.15)))
        group = [d for d in remaining if abs(d - seed) <= tol]
        remaining = [d for d in remaining if d not in group]

        med = int(round(_median([float(x) for x in group]) or seed))
        clusters.append({"median_s": med, "count": len(group), "members": sorted(group)})

    # Tri décroissant par durée
    clusters.sort(key=lambda c: c["median_s"], reverse=True)
    return clusters


def build_intervals_structure_v4(activity_id: int, db_path: str = "running.db") -> Dict[str, Any]:
    pts = _fetch_stream_points(activity_id, db_path=db_path)
    if not pts:
        raise RuntimeError("Aucun stream_points pour cette activité.")

    # séries
    ts = [p[1] for p in pts]
    ds = [p[2] for p in pts]
    vs = [p[3] for p in pts]

    # params effort (conservateurs)
    smooth_window = 9
    z_eff_on, z_eff_off = 1.0, 0.4
    min_eff_s = 18          # 30" => 30s donc OK; 18s laisse un peu de marge
    min_eff_dist_m = 40.0   # on baisse pour permettre 30" sur place/accélérations courtes
    merge_gap_s = 3

    # lissage
    vs_clean = [float(v) if isinstance(v, (int, float)) else None for v in vs]
    vs_s = _rolling_mean(vs_clean, window=smooth_window)

    valid = [v for v in vs_s if isinstance(v, (int, float)) and v > 0.5]
    if len(valid) < 60:
        return {"note": "Not enough valid speed points", "effort_count": 0}

    v_med, v_mad = _median_mad(valid)
    z = [_robust_z(v, v_med, v_mad) for v in vs_s]

    # label effort
    labels = ["NEUTRAL"] * len(z)
    state = "NEUTRAL"
    for i, zi in enumerate(z):
        if zi is None:
            labels[i] = "NEUTRAL"
            state = "NEUTRAL"
            continue
        if state == "NEUTRAL":
            if zi >= z_eff_on:
                state = "EFFORT"
        else:
            if zi <= z_eff_off:
                state = "NEUTRAL"
        labels[i] = state

    # blocs label
    blocks: List[Tuple[str, int, int]] = []
    start = 0
    cur = labels[0]
    for i in range(1, len(labels)):
        if labels[i] != cur:
            blocks.append((cur, start, i - 1))
            start = i
            cur = labels[i]
    blocks.append((cur, start, len(labels) - 1))

    # efforts filtrés
    eff_raw: List[Tuple[int, int]] = []
    for (lab, s, e) in blocks:
        if lab != "EFFORT":
            continue
        t0, t1 = ts[s], ts[e]
        d0, d1 = ds[s], ds[e]
        if t0 is None or t1 is None or d0 is None or d1 is None:
            continue
        dur = int(max(0, t1 - t0))
        dist_seg = float(max(0.0, d1 - d0))
        if dur < min_eff_s:
            continue
        if dist_seg < min_eff_dist_m:
            continue
        eff_raw.append((s, e))

    # merge efforts proches
    eff_blocks: List[Tuple[int, int]] = []
    for (s, e) in eff_raw:
        if not eff_blocks:
            eff_blocks.append((s, e))
            continue
        ps, pe = eff_blocks[-1]
        gap = s - pe - 1
        if gap <= merge_gap_s:
            eff_blocks[-1] = (ps, e)
        else:
            eff_blocks.append((s, e))

    # si pas d'efforts => rien à structurer
    if not eff_blocks:
        # nettoie éventuels anciens
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("DELETE FROM laps_auto WHERE activity_id = ? AND lap_type IN ('WARMUP','EFFORT','RECUP','COOLDOWN');", (activity_id,))
        conn.commit()
        conn.close()
        return {"effort_count": 0, "recup_count": 0, "note": "No efforts detected"}

    # warmup/cooldown (tout le reste)
    warmup = (0, eff_blocks[0][0] - 1) if eff_blocks[0][0] > 0 else None
    cooldown = (eff_blocks[-1][1] + 1, len(pts) - 1) if eff_blocks[-1][1] < len(pts) - 1 else None

    # recups = gaps entre efforts (variable trot/marche OK)
    rec_blocks: List[Tuple[int, int]] = []
    for i in range(len(eff_blocks) - 1):
        s1, e1 = eff_blocks[i]
        s2, _ = eff_blocks[i + 1]
        rs = e1 + 1
        re = s2 - 1
        if rs < re:
            rec_blocks.append((rs, re))

    # écrire DB (idempotent sur ces types)
    rows_to_insert: List[Tuple] = []
    lap_idx = 1

    if warmup and warmup[0] <= warmup[1]:
        rows_to_insert.append(_make_row(activity_id, "WARMUP", 1, pts, warmup[0], warmup[1]))

    for i, (s, e) in enumerate(eff_blocks):
        rows_to_insert.append(_make_row(activity_id, "EFFORT", i + 1, pts, s, e))

    for i, (s, e) in enumerate(rec_blocks):
        rows_to_insert.append(_make_row(activity_id, "RECUP", i + 1, pts, s, e))

    if cooldown and cooldown[0] <= cooldown[1]:
        rows_to_insert.append(_make_row(activity_id, "COOLDOWN", 1, pts, cooldown[0], cooldown[1]))

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("DELETE FROM laps_auto WHERE activity_id = ? AND lap_type IN ('WARMUP','EFFORT','RECUP','COOLDOWN');", (activity_id,))
    cur.executemany("""
        INSERT INTO laps_auto (
            activity_id, lap_index, lap_type, start_idx, end_idx,
            start_time_s, end_time_s, duration_s, distance_m,
            pace_s_per_km, avg_hr, avg_grade
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, rows_to_insert)
    conn.commit()
    conn.close()

    # --- structure time-based (console) : cluster par durées effort
    effort_durations = []
    for (s, e) in eff_blocks:
        t0, t1 = ts[s], ts[e]
        if t0 is None or t1 is None:
            effort_durations.append(int(max(0, e - s)))
        else:
            effort_durations.append(int(max(0, t1 - t0)))

    clusters = _cluster_efforts_by_duration(effort_durations)

    # relecture séquentielle: assign cluster par proximité
    def nearest_cluster(d: int) -> int:
        best_i = 0
        best_err = 10**9
        for i, c in enumerate(clusters):
            err = abs(d - c["median_s"])
            if err < best_err:
                best_err = err
                best_i = i
        return best_i

    seq = [nearest_cluster(d) for d in effort_durations]

    # group consecutive sets
    sets = []
    if seq:
        cur_id = seq[0]
        count = 1
        for i in range(1, len(seq)):
            if seq[i] == cur_id:
                count += 1
            else:
                sets.append((cur_id, count))
                cur_id = seq[i]
                count = 1
        sets.append((cur_id, count))

    return {
        "effort_count": len(eff_blocks),
        "recup_count": len(rec_blocks),
        "warmup": warmup is not None,
        "cooldown": cooldown is not None,
        "v_median": v_med,
        "v_mad": v_mad,
        "params": {
            "smooth_window": smooth_window,
            "z_eff_on": z_eff_on, "z_eff_off": z_eff_off,
            "min_eff_s": min_eff_s, "min_eff_dist_m": min_eff_dist_m,
            "merge_gap_s": merge_gap_s
        },
        "clusters": clusters,
        "sets": [{"median_s": clusters[cid]["median_s"], "count": cnt} for (cid, cnt) in sets]
    }


# -----------------------
# Demo
# -----------------------
def main_demo():
    db_path = "running.db"
    init_db(db_path)

    print_recent_runs_with_streams(db_path, limit=20)
    print("Choisis un activity_id dans la liste ci-dessus (copie-colle le nombre).")
    print("Option: si tu laisses vide, je prends automatiquement la plus récente.\n")

    user_in = input("activity_id = ").strip()
    if user_in:
        activity_id = int(user_in)
    else:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT activity_id
            FROM activities
            WHERE sport_type IN ('Run', 'Trail Run') AND streams_status = 'OK'
            ORDER BY start_date_local DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
        conn.close()
        if not row:
            raise RuntimeError("Aucune activité RUN avec streams_status=OK en base.")
        activity_id = int(row[0])

    print("\nActivity sélectionnée =", activity_id)

    res = build_intervals_structure_v4(activity_id, db_path=db_path)
    print("\nRésultat V4:", res)

    # affichage lisible de la structure
    if "sets" in res and res["sets"]:
        print("\nStructure détectée (approx):")
        for i, s in enumerate(res["sets"], start=1):
            print(f"  Bloc {i}: {s['count']} × ~{s['median_s']}s")
    else:
        print("\nStructure détectée: (aucune)")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print("\nExemple (WARMUP):")
    cur.execute("""
        SELECT lap_index, duration_s, distance_m, pace_s_per_km, avg_hr
        FROM laps_auto
        WHERE activity_id = ? AND lap_type = 'WARMUP'
        ORDER BY lap_index ASC;
    """, (activity_id,))
    for r in cur.fetchall():
        print(r)

    print("\nExemple (5 premiers EFFORT):")
    cur.execute("""
        SELECT lap_index, duration_s, distance_m, pace_s_per_km, avg_hr
        FROM laps_auto
        WHERE activity_id = ? AND lap_type = 'EFFORT'
        ORDER BY lap_index ASC
        LIMIT 5;
    """, (activity_id,))
    for r in cur.fetchall():
        print(r)

    print("\nExemple (5 premiers RECUP):")
    cur.execute("""
        SELECT lap_index, duration_s, distance_m, pace_s_per_km, avg_hr
        FROM laps_auto
        WHERE activity_id = ? AND lap_type = 'RECUP'
        ORDER BY lap_index ASC
        LIMIT 5;
    """, (activity_id,))
    for r in cur.fetchall():
        print(r)

    print("\nExemple (COOLDOWN):")
    cur.execute("""
        SELECT lap_index, duration_s, distance_m, pace_s_per_km, avg_hr
        FROM laps_auto
        WHERE activity_id = ? AND lap_type = 'COOLDOWN'
        ORDER BY lap_index ASC;
    """, (activity_id,))
    for r in cur.fetchall():
        print(r)

    conn.close()


if __name__ == "__main__":
    main_demo()
