import argparse
from datetime import datetime

from app.dashboard.db import connect_db, ensure_dashboard_tables, q


def parse_args():
    parser = argparse.ArgumentParser(description="Running Dashboard")
    parser.add_argument(
        "--mode",
        choices=["principal", "advanced"],
        required=True,
        help="Dashboard mode",
    )
    parser.add_argument(
        "--period",
        choices=["month", "year", "all"],
        default="month",
        help="Time period filter (applies to list views).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit for advanced list view.",
    )
    parser.add_argument(
        "--activity-id",
        type=int,
        default=None,
        help="Show details for a single activity_id (advanced mode).",
    )
    return parser.parse_args()


def get_period_filter(period: str):
    """
    Returns (where_sql_fragment, params_tuple)
    activities.start_date_local is stored as ISO string (e.g., 2025-12-26T16:30:15Z).
    """
    if period == "all":
        return "", ()
    now = datetime.now()
    if period == "year":
        dt = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        return "AND a.start_date_local >= ?", (dt,)
    if period == "month":
        dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        return "AND a.start_date_local >= ?", (dt,)
    return "", ()


def _print_header(title: str):
    print("\n================ DASHBOARD ================\n")
    print(f"{title}\n")


def _base_cte(where_clause: str):
    """
    CTE used by principal/advanced to compute per-activity metrics from stream_points.
    - distance_m = max(distance_m)
    - time_s = max(time_s)
    - avg_hr = avg(heartrate_bpm)
    """
    return f"""
    WITH filtered_activities AS (
        SELECT a.activity_id, a.type, a.start_date_local, a.name
        FROM activities a
        WHERE 1=1 {where_clause}
    ),
    sp_stats AS (
        SELECT
            sp.activity_id,
            MAX(sp.distance_m) AS distance_m,
            MAX(sp.time_s) AS time_s,
            AVG(sp.heartrate_bpm) AS avg_hr
        FROM stream_points sp
        GROUP BY sp.activity_id
    ),
    activity_metrics AS (
        SELECT
            fa.activity_id,
            fa.type,
            fa.start_date_local,
            fa.name,
            ss.distance_m,
            ss.time_s,
            ss.avg_hr
        FROM filtered_activities fa
        LEFT JOIN sp_stats ss ON ss.activity_id = fa.activity_id
    )
    """


def dashboard_principal(conn, period: str):
    where_clause, params = get_period_filter(period)
    period_title = period.upper() if period != "all" else "ALL TIME"
    _print_header(f"DASHBOARD — MODE PRINCIPAL | PÉRIODE: {period_title}")

    base_cte = _base_cte(where_clause)

    # ---- BLOC A : activité globale multisport ----
    rows = q(
        conn,
        base_cte
        + """
        SELECT
            am.type,
            COUNT(*) AS n_sessions,
            ROUND(SUM(COALESCE(am.distance_m, 0)) / 1000.0, 1) AS km
        FROM activity_metrics am
        GROUP BY am.type
        ORDER BY km DESC
        """,
        params,
    )

    print("BLOC A — Activité globale (multisport)")
    if rows:
        for sport, n, km in rows:
            print(f"  {sport:15s} | séances: {int(n):3d} | distance: {float(km or 0):6.1f} km")
    else:
        print("  (aucune activité sur la période)")
    print()

    # ---- BLOC B : volume course ----
    rows = q(
        conn,
        base_cte
        + """
        SELECT
            COUNT(*) AS n_sessions,
            ROUND(SUM(COALESCE(distance_m, 0)) / 1000.0, 1) AS km
        FROM activity_metrics
        WHERE type = 'Run'
        """,
        params,
    )
    n_run, km_run = rows[0]
    print("BLOC B — Volume course (Run)")
    print(f"  Séances RUN : {int(n_run)}")
    print(f"  Distance    : {float(km_run or 0):.1f} km\n")

    # ---- BLOC C : intensité déclarée (minutes + %) ----
    rows = q(
        conn,
        f"""
        WITH filtered_activities AS (
            SELECT a.activity_id
            FROM activities a
            WHERE 1=1 {where_clause}
        )
        SELECT
            si.bucket,
            SUM(si.seconds) AS seconds
        FROM session_intensity si
        JOIN filtered_activities fa ON fa.activity_id = si.activity_id
        GROUP BY si.bucket
        """,
        params,
    )

    print("BLOC C — Répartition intensité déclarée (déclaratif)")
    if rows:
        total_s = sum(float(r[1] or 0) for r in rows)
        total_min = total_s / 60.0 if total_s > 0 else 0.0
        print(f"  Total déclaré : {total_min:.1f} min")
        order = {"E": 1, "T": 2, "I": 3, "S": 4, "V": 5}
        rows_sorted = sorted(rows, key=lambda x: order.get(x[0], 99))
        for bucket, seconds in rows_sorted:
            sec = float(seconds or 0)
            minutes = sec / 60.0
            pct = (sec / total_s * 100.0) if total_s > 0 else 0.0
            print(f"  {bucket}: {minutes:6.1f} min | {pct:5.1f}%")
        print("  Note: intensité = saisie manuelle via CSV (pas calculée).")
    else:
        print("  (aucune intensité déclarée sur la période)")
    print()

    # ---- BLOC D : charge interne (Run-only) ----
    rows = q(
        conn,
        base_cte
        + """
        SELECT
            ROUND(SUM(COALESCE(sr.rpe, 0) * (COALESCE(am.time_s, 0) / 60.0)), 1) AS load,
            SUM(CASE WHEN sr.rpe IS NOT NULL THEN 1 ELSE 0 END) AS n_with_rpe,
            COUNT(*) AS n_total
        FROM activity_metrics am
        LEFT JOIN session_rpe sr ON sr.activity_id = am.activity_id
        WHERE am.type = 'Run'
        """,
        params,
    )
    load, n_with_rpe, n_total = rows[0]
    print("BLOC D — Charge interne (RPE × durée) — Run")
    print(f"  Charge totale : {float(load or 0):.1f}")
    print(f"  Couverture RPE: {int(n_with_rpe)} / {int(n_total)} séances Run sur la période\n")

    # ---- BLOC E : performance factuelle (Run) ----
    rows = q(
        conn,
        base_cte
        + """
        SELECT
            ROUND(AVG(CASE
                WHEN COALESCE(time_s, 0) > 0 THEN (distance_m / time_s) * 3.6
                ELSE NULL
            END), 2) AS avg_kmh,
            ROUND(AVG(avg_hr), 1) AS avg_hr
        FROM activity_metrics
        WHERE type = 'Run'
        """,
        params,
    )
    avg_kmh, avg_hr = rows[0]
    print("BLOC E — Performance factuelle (Run)")
    print(f"  Vitesse moyenne : {float(avg_kmh or 0):.2f} km/h")
    if avg_hr is not None:
        print(f"  FC moyenne      : {float(avg_hr):.1f} bpm")
    else:
        print("  FC moyenne      : (non disponible)")
    print()

    # ---- BLOC F : dénivelé total (D+) sur Run ----
    rows = q(
        conn,
        f"""
        WITH filtered_runs AS (
            SELECT a.activity_id
            FROM activities a
            WHERE a.type = 'Run' {where_clause}
        ),
        sp_lag AS (
            SELECT
                sp.activity_id,
                sp.idx,
                sp.altitude_m,
                sp.altitude_m - LAG(sp.altitude_m) OVER (
                    PARTITION BY sp.activity_id
                    ORDER BY sp.idx
                ) AS dalt
            FROM stream_points sp
            JOIN filtered_runs fr ON fr.activity_id = sp.activity_id
            WHERE sp.altitude_m IS NOT NULL
        ),
        dplus_by_activity AS (
            SELECT
                activity_id,
                SUM(CASE WHEN dalt > 0 THEN dalt ELSE 0 END) AS dplus_m
            FROM sp_lag
            GROUP BY activity_id
        )
        SELECT ROUND(SUM(COALESCE(dplus_m, 0)), 0) AS dplus_total_m
        FROM dplus_by_activity
        """,
        params,
    )
    dplus_total = rows[0][0]
    print("BLOC F — Terrain / dénivelé (Run)")
    print(f"  D+ total : {float(dplus_total or 0):.0f} m\n")

    # ---- BLOC G : continuité long terme (factuel) ----
    rows = q(
        conn,
        """
        SELECT start_date_local
        FROM activities
        WHERE type = 'Run'
        ORDER BY start_date_local ASC
        """
    )

    print("BLOC G — Continuité long terme (Run, factuel)")
    if not rows:
        print("  (aucune séance Run en base)")
    else:
        dates = []
        for (s,) in rows:
            if not s:
                continue
            ss = s[:-1] if s.endswith("Z") else s
            try:
                dt = datetime.fromisoformat(ss)
                dates.append(dt)
            except ValueError:
                continue

        if not dates:
            print("  (dates illisibles)")
        else:
            active_weeks = set((d.isocalendar().year, d.isocalendar().week) for d in dates)
            weeks_sorted = sorted(active_weeks)

            def week_index(yw):
                y, w = yw
                dt = datetime.fromisocalendar(y, w, 1)
                return dt.toordinal() // 7

            weeks_active = len(weeks_sorted)
            longest = 1
            cur = 1
            for i in range(1, len(weeks_sorted)):
                if week_index(weeks_sorted[i]) == week_index(weeks_sorted[i - 1]) + 1:
                    cur += 1
                    longest = max(longest, cur)
                else:
                    cur = 1

            today = datetime.now()
            this_week = (today.isocalendar().year, today.isocalendar().week)
            last_week = max(weeks_sorted, key=week_index)
            since_last = week_index(this_week) - week_index(last_week)
            if since_last < 0:
                since_last = 0

            print(f"  Semaines actives (≥1 Run) : {weeks_active}")
            print(f"  Plus longue série (semaines consécutives) : {longest}")
            print(f"  Semaines depuis dernière semaine active   : {since_last}")
    print()

    print("============================================================\n")


def _format_date(s: str) -> str:
    if not s:
        return ""
    return s[:10]


def advanced_list(conn, period: str, limit: int):
    where_clause, params = get_period_filter(period)
    period_title = period.upper() if period != "all" else "ALL TIME"
    _print_header(f"DASHBOARD — MODE ADVANCED (LIST) | PÉRIODE: {period_title} | LIMIT: {limit}")

    base_cte = _base_cte(where_clause)

    # per-activity declared intensity minutes
    intensity_rows = q(
        conn,
        f"""
        WITH filtered_activities AS (
            SELECT a.activity_id
            FROM activities a
            WHERE 1=1 {where_clause}
        )
        SELECT si.activity_id, si.bucket, ROUND(SUM(si.seconds)/60.0, 1) AS minutes
        FROM session_intensity si
        JOIN filtered_activities fa ON fa.activity_id = si.activity_id
        GROUP BY si.activity_id, si.bucket
        """,
        params,
    )
    intensity_map = {}
    for aid, bucket, minutes in intensity_rows:
        intensity_map.setdefault(int(aid), {})[bucket] = float(minutes or 0)

    rows = q(
        conn,
        base_cte
        + """
        SELECT
            am.activity_id,
            am.start_date_local,
            am.type,
            am.name,
            ROUND(COALESCE(am.distance_m, 0) / 1000.0, 2) AS km,
            ROUND(COALESCE(am.time_s, 0) / 60.0, 1) AS minutes,
            ROUND(am.avg_hr, 1) AS avg_hr,
            sr.rpe,
            sc.terrain_type,
            sc.shoes
        FROM activity_metrics am
        LEFT JOIN session_rpe sr ON sr.activity_id = am.activity_id
        LEFT JOIN session_context sc ON sc.activity_id = am.activity_id
        ORDER BY am.start_date_local DESC
        LIMIT ?
        """,
        params + (limit,),
    )

    if not rows:
        print("(aucune activité)")
        return

    for (
        activity_id,
        start_date_local,
        typ,
        name,
        km,
        minutes,
        avg_hr,
        rpe,
        terrain_type,
        shoes,
    ) in rows:
        aid = int(activity_id)
        date_s = _format_date(start_date_local)
        inten = intensity_map.get(aid, {})
        inten_str = ""
        if inten:
            order = ["E", "T", "I", "S", "V"]
            parts = [f"{k}:{inten.get(k, 0):.1f}" for k in order if k in inten]
            inten_str = " | INT(" + " ".join(parts) + ")"
        rpe_str = f" | RPE:{int(rpe)}" if rpe is not None else ""
        terr_str = f" | {terrain_type}" if terrain_type else ""
        shoes_str = f" | {shoes}" if shoes else ""
        hr_str = f" | HR:{float(avg_hr):.1f}" if avg_hr is not None else ""
        print(f"- {aid} | {date_s} | {typ} | {name} | {float(km):.2f} km | {float(minutes):.1f} min{hr_str}{rpe_str}{terr_str}{shoes_str}{inten_str}")

    print("\nTip: pour le détail d’une séance:")
    print("  python -m app.dashboard.dashboard --mode advanced --activity-id <ID>\n")


def advanced_detail(conn, activity_id: int):
    _print_header(f"DASHBOARD — MODE ADVANCED (DETAIL) | ACTIVITY_ID: {activity_id}")

    # Basic activity info
    rows = q(
        conn,
        """
        SELECT a.activity_id, a.start_date_local, a.type, a.name
        FROM activities a
        WHERE a.activity_id = ?
        """,
        (activity_id,),
    )
    if not rows:
        print("(activité introuvable)")
        return

    aid, start_date_local, typ, name = rows[0]

    # Stream-derived metrics
    metrics = q(
        conn,
        """
        SELECT
            ROUND(MAX(distance_m)/1000.0, 2) AS km,
            ROUND(MAX(time_s)/60.0, 1) AS minutes,
            ROUND(AVG(heartrate_bpm), 1) AS avg_hr
        FROM stream_points
        WHERE activity_id = ?
        """,
        (activity_id,),
    )[0]
    km, minutes, avg_hr = metrics

    # D+ via altitude deltas
    dplus = q(
        conn,
        """
        WITH sp_lag AS (
            SELECT
                idx,
                altitude_m,
                altitude_m - LAG(altitude_m) OVER (ORDER BY idx) AS dalt
            FROM stream_points
            WHERE activity_id = ? AND altitude_m IS NOT NULL
        )
        SELECT ROUND(SUM(CASE WHEN dalt > 0 THEN dalt ELSE 0 END), 0) FROM sp_lag
        """,
        (activity_id,),
    )[0][0]

    # Context / RPE / notes
    ctx = q(
        conn,
        "SELECT terrain_type, shoes, context_note FROM session_context WHERE activity_id = ?",
        (activity_id,),
    )
    terrain_type, shoes, context_note = ctx[0] if ctx else (None, None, None)

    rpe_row = q(
        conn,
        "SELECT rpe, rpe_note FROM session_rpe WHERE activity_id = ?",
        (activity_id,),
    )
    rpe, rpe_note = rpe_row[0] if rpe_row else (None, None)

    inten_rows = q(
        conn,
        """
        SELECT bucket, ROUND(seconds/60.0, 1) AS minutes
        FROM session_intensity
        WHERE activity_id = ?
        ORDER BY bucket
        """,
        (activity_id,),
    )
    inten_note_row = q(
        conn,
        "SELECT intensity_note FROM session_intensity_note WHERE activity_id = ?",
        (activity_id,),
    )
    intensity_note = inten_note_row[0][0] if inten_note_row else None

    print(f"ID        : {int(aid)}")
    print(f"Date      : {_format_date(start_date_local)}")
    print(f"Type      : {typ}")
    print(f"Nom       : {name}")
    print(f"Distance  : {float(km or 0):.2f} km")
    print(f"Durée     : {float(minutes or 0):.1f} min")
    print(f"D+        : {float(dplus or 0):.0f} m")
    if avg_hr is not None:
        print(f"FC moy    : {float(avg_hr):.1f} bpm")
    else:
        print("FC moy    : (non disponible)")

    print("\nSaisie manuelle (CSV)")
    print(f"Terrain   : {terrain_type or '(vide)'}")
    print(f"Chaussures: {shoes or '(vide)'}")
    print(f"RPE       : {rpe if rpe is not None else '(vide)'}")
    if rpe_note:
        print(f"RPE note  : {rpe_note}")
    if context_note:
        print(f"Contexte  : {context_note}")

    print("\nIntensité déclarée (minutes)")
    if inten_rows:
        for bucket, minutes in inten_rows:
            print(f"  {bucket}: {float(minutes or 0):.1f} min")
    else:
        print("  (aucune intensité déclarée)")
    if intensity_note:
        print(f"Note intensité: {intensity_note}")

    print("\n============================================================\n")


def main():
    args = parse_args()
    conn = connect_db()
    ensure_dashboard_tables(conn)

    if args.mode == "principal":
        dashboard_principal(conn, args.period)
    else:
        if args.activity_id is not None:
            advanced_detail(conn, args.activity_id)
        else:
            advanced_list(conn, args.period, args.limit)

    conn.close()


if __name__ == "__main__":
    main()
