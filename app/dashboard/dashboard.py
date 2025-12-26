from __future__ import annotations

import argparse
import datetime as dt
from typing import Dict, Any, Optional

from .db import connect, ensure_dashboard_tables, q
from .metrics import (
    fmt_hms, fmt_km, fmt_pace, fmt_float, fmt_int,
    list_recent_runs, get_activity_meta, get_activity_context, get_session_rpe,
    get_session_intensity_declared, activity_totals_from_streams,
    group_counts_by_sport, run_days_and_off_days
)


# -----------------------------
# Time helpers
# -----------------------------

def iso_day(d: dt.date) -> str:
    return d.isoformat()


def iso_dt(d: dt.datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%S")


def month_range(any_day: dt.date) -> tuple[str, str]:
    start = any_day.replace(day=1)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1, day=1)
    else:
        end = start.replace(month=start.month + 1, day=1)
    return iso_day(start), iso_day(end)


def year_range(any_day: dt.date) -> tuple[str, str]:
    start = any_day.replace(month=1, day=1)
    end = start.replace(year=start.year + 1, month=1, day=1)
    return iso_day(start), iso_day(end)


# -----------------------------
# Dashboard blocks (A..G)
# -----------------------------

def block_a_global(conn, date_from: str, date_to: str) -> None:
    print("\n[BLOC A — Activité globale (multisport)]")
    counts = group_counts_by_sport(conn, date_from, date_to)
    total = sum(counts.values())
    print(f"  Séances totales : {total}")
    if not counts:
        print("  Répartition par sport : —")
        return
    print("  Répartition par sport :")
    for k in sorted(counts.keys()):
        print(f"    - {k}: {counts[k]}")


def block_b_volume_run(conn, date_from: str, date_to: str) -> None:
    print("\n[BLOC B — Volume & régularité (course à pied)]")

    # km totaux Run/Trail Run via activities.distance si dispo sinon via streams
    rows = q(conn, """
        SELECT activity_id
        FROM activities
        WHERE sport_type IN ('Run','Trail Run')
          AND start_date_local >= ? AND start_date_local < ?
          AND streams_status='OK';
    """, (date_from, date_to))

    total_m = 0.0
    n_ok = 0
    for r in rows:
        tot = activity_totals_from_streams(conn, int(r["activity_id"]))
        if tot["dist_m"] is not None:
            total_m += float(tot["dist_m"])
            n_ok += 1

    days = run_days_and_off_days(conn, date_from, date_to)
    print(f"  Kilomètres totaux (streams) : {fmt_km(total_m)} (sur {n_ok} séances RUN/TRAIL avec streams)")
    print(f"  Jours courus : {days['n_days_with_run']}")
    print("  Note : pas de records, pas de scoring (neutralité).")


def block_c_intensity(conn, date_from: str, date_to: str) -> None:
    print("\n[BLOC C — Répartition de l’intensité (E/T/I/S/V)]")
    print("  Source: table session_intensity (déclaré). Hybride autorisé (plusieurs buckets).")

    rows = q(conn, """
        SELECT a.activity_id
        FROM activities a
        WHERE a.start_date_local >= ? AND a.start_date_local < ?;
    """, (date_from, date_to))

    agg = {"E": 0.0, "T": 0.0, "I": 0.0, "S": 0.0, "V": 0.0}
    n_labeled = 0
    for r in rows:
        d = get_session_intensity_declared(conn, int(r["activity_id"]))
        if d:
            n_labeled += 1
            for k, v in d.items():
                if k in agg:
                    agg[k] += float(v)

    if n_labeled == 0:
        print("  — Aucune répartition renseignée pour la période.")
        print("  Action: tu pourras remplir session_intensity plus tard (sans IA, factuel).")
        return

    total = sum(agg.values())
    for k in ["E", "T", "I", "S", "V"]:
        pct = (agg[k] / total * 100.0) if total > 0 else None
        print(f"  {k}: {fmt_hms(agg[k])}  ({fmt_float(pct,1)}%)")
    print(f"  Séances renseignées: {n_labeled}")


def block_d_load_recovery(conn, date_from: str, date_to: str) -> None:
    print("\n[BLOC D — Charge globale & récupération]")
    print("  Pas de score unique, pas de seuil danger. Tendances uniquement.")

    rows = q(conn, """
        SELECT activity_id
        FROM activities
        WHERE start_date_local >= ? AND start_date_local < ?;
    """, (date_from, date_to))

    # Charge interne: RPE × durée (si RPE renseigné + durée approx via n_points)
    internal = 0.0
    internal_n = 0
    dist_total = 0.0
    dplus_total = 0.0

    for r in rows:
        aid = int(r["activity_id"])
        tot = activity_totals_from_streams(conn, aid)
        if tot["dist_m"] is not None:
            dist_total += float(tot["dist_m"])
        dplus_total += float(tot["dplus_m"] or 0.0)

        rpe = get_session_rpe(conn, aid)
        if rpe and rpe["rpe"] is not None:
            # duration proxy = n_points seconds (factuel: "n_points")
            duration_s = float(tot["n_points"])
            internal += float(rpe["rpe"]) * duration_s
            internal_n += 1

    print(f"  Charge externe (distance, streams) : {fmt_km(dist_total)}")
    print(f"  Charge externe (D+) : {fmt_int(dplus_total)} m")
    if internal_n == 0:
        print("  Charge interne (RPE×durée) : — (RPE non renseigné sur la période)")
    else:
        print(f"  Charge interne (RPE×durée, proxy durée=points) : {fmt_float(internal,0)} (sur {internal_n} séances avec RPE)")


def block_e_performance_context(conn, date_from: str, date_to: str) -> None:
    print("\n[BLOC E — Performance contextualisée (course)]")
    print("  Règles: comparaisons strictement homogènes, pas de records mis en avant.")

    # V1 minimal: afficher quelques références factuelles "effort comparable" via endurance runs
    # Ici: on ne peut pas inférer E vs qualité; donc on s'appuie sur session_intensity si présent.
    rows = q(conn, """
        SELECT a.activity_id, a.start_date_local, a.name
        FROM activities a
        WHERE a.sport_type IN ('Run','Trail Run')
          AND a.start_date_local >= ? AND a.start_date_local < ?
          AND a.streams_status='OK'
        ORDER BY a.start_date_local DESC
        LIMIT 10;
    """, (date_from, date_to))

    if not rows:
        print("  — Aucune séance RUN/TRAIL avec streams.")
        return

    print("  Dernières séances RUN/TRAIL (références factuelles: pace global + HR moyen si dispo):")
    for r in rows:
        aid = int(r["activity_id"])
        tot = activity_totals_from_streams(conn, aid)
        # duration proxy = n_points
        duration_s = float(tot["n_points"])
        pace = fmt_pace((duration_s / tot["dist_m"] * 1000.0) if tot["dist_m"] else None)
        print(f"    - {aid} | {r['start_date_local']} | {r['name']} | dist {fmt_km(tot['dist_m'])} | pace~ {pace} | HR_avg {fmt_float(tot['hr_avg'],1)}")


def block_f_terrain_mech(conn, date_from: str, date_to: str) -> None:
    print("\n[BLOC F — Terrain & contrainte mécanique]")
    print("  Règles: descriptif. La variabilité n’est pas un problème par défaut.")

    rows = q(conn, """
        SELECT activity_id, start_date_local, name, sport_type
        FROM activities
        WHERE sport_type IN ('Run','Trail Run')
          AND start_date_local >= ? AND start_date_local < ?
          AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT 10;
    """, (date_from, date_to))

    if not rows:
        print("  — Aucune séance RUN/TRAIL avec streams.")
        return

    for r in rows:
        aid = int(r["activity_id"])
        ctx = get_activity_context(conn, aid)
        terrain = ctx["terrain_type"] if ctx and ctx["terrain_type"] else "non renseigné"
        tot = activity_totals_from_streams(conn, aid)
        print(f"  - {aid} | {r['start_date_local']} | {r['name']} | sport={r['sport_type']} | terrain={terrain} | D+ {fmt_int(tot['dplus_m'])}m | D- {fmt_int(tot['dminus_m'])}m")


def block_g_continuity(conn, date_from: str, date_to: str) -> None:
    print("\n[BLOC G — Continuité & cohérence long terme]")
    print("  Objectif: lecture longitudinale, sans interprétation automatique.")

    # V1: continuité hebdo = nb de semaines actives (au moins 1 run) + ruptures (>=7j sans run)
    rows = q(conn, """
        SELECT substr(start_date_local, 1, 10) AS day
        FROM activities
        WHERE sport_type IN ('Run','Trail Run')
          AND start_date_local >= ? AND start_date_local < ?
        ORDER BY day ASC;
    """, (date_from, date_to))

    if not rows:
        print("  — Aucun RUN/TRAIL sur la période.")
        return

    # parse days
    days = [dt.date.fromisoformat(r["day"]) for r in rows if r["day"]]
    days = sorted(set(days))

    # ruptures >= 7 jours
    ruptures = []
    for i in range(1, len(days)):
        gap = (days[i] - days[i - 1]).days
        if gap >= 7:
            ruptures.append((days[i - 1], days[i], gap))

    # weeks active
    weeks = set()
    for d in days:
        iso = d.isocalendar()
        weeks.add((iso.year, iso.week))
    print(f"  Semaines actives (>=1 run): {len(weeks)}")
    print(f"  Jours avec run: {len(days)}")

    if not ruptures:
        print("  Ruptures (>=7 jours sans run): aucune détectée.")
    else:
        print("  Ruptures (>=7 jours sans run):")
        for a, b, gap in ruptures:
            print(f"    - {a.isoformat()} -> {b.isoformat()} : {gap} jours")


# -----------------------------
# Entry points: principal + advanced
# -----------------------------

def dashboard_principal(conn, period: str) -> None:
    """
    Parcours guidé A -> G, une page, restreint.
    """
    today = dt.date.today()

    if period == "month":
        d0, d1 = month_range(today)
    elif period == "year":
        d0, d1 = year_range(today)
    else:
        # default: current month
        d0, d1 = month_range(today)

    print("=" * 96)
    print("DASHBOARD PRINCIPAL (V1) — lecture guidée, factuelle, non prescriptive")
    print(f"Période: {d0} -> {d1} (exclusive)")
    print("=" * 96)

    # ordre A -> G (section 18.6)
    block_a_global(conn, d0, d1)
    block_b_volume_run(conn, d0, d1)
    block_c_intensity(conn, d0, d1)
    block_d_load_recovery(conn, d0, d1)
    block_e_performance_context(conn, d0, d1)
    block_f_terrain_mech(conn, d0, d1)
    block_g_continuity(conn, d0, d1)

    print("\n(Conforme V1: pas de recommandations, pas de scoring, pas de records.)")


def dashboard_advanced(conn) -> None:
    """
    Exploration libre, mêmes données, plus de granularité.
    Fonctions autorisées (section 18.5): drill-down séance → laps, filtres avancés, comparaisons manuelles de périodes.
    Interdit: interprétation automatique/reco/scoring.
    """
    print("=" * 96)
    print("DASHBOARD ANALYTIQUE AVANCÉ (V1) — exploration libre (sans interprétation)")
    print("=" * 96)
    print("\nCommandes disponibles:")
    print("  1) Lister dernières séances RUN/TRAIL (streams OK)")
    print("  2) Afficher meta + contexte + RPE d’une séance")
    print("  3) Afficher répartition E/T/I/S/V déclarée d’une séance (si renseignée)")
    print("  0) Quitter")

    while True:
        choice = input("\nChoix = ").strip()
        if choice == "0":
            break

        if choice == "1":
            rows = list_recent_runs(conn, 20)
            for r in rows:
                print(f"{r['activity_id']} | {r['start_date_local']} | {r['sport_type']} | {r['name']}")
            continue

        if choice == "2":
            aid = int(input("activity_id = ").strip())
            meta = get_activity_meta(conn, aid)
            if not meta:
                print("— activity_id inconnu.")
                continue
            ctx = get_activity_context(conn, aid)
            rpe = get_session_rpe(conn, aid)
            tot = activity_totals_from_streams(conn, aid)

            print(f"\nMETA: {meta['activity_id']} | {meta['start_date_local']} | {meta['name']} | {meta['sport_type']}")
            print(f"  device={meta['device_name']} | has_hr={meta['has_heartrate']}")
            print(f"  dist(streams)={fmt_km(tot['dist_m'])} | D+={fmt_int(tot['dplus_m'])}m | D-={fmt_int(tot['dminus_m'])}m | HR_avg={fmt_float(tot['hr_avg'],1)}")
            if ctx:
                print(f"  terrain={ctx['terrain_type'] or '—'} | note={ctx['surface_note'] or '—'} | shoes={ctx['shoes'] or '—'}")
            else:
                print("  terrain=— | note=— | shoes=— (non renseigné)")

            if rpe and rpe["rpe"] is not None:
                print(f"  RPE={rpe['rpe']} | note={rpe['note'] or '—'}")
            else:
                print("  RPE=— (non renseigné)")
            continue

        if choice == "3":
            aid = int(input("activity_id = ").strip())
            d = get_session_intensity_declared(conn, aid)
            if not d:
                print("— aucune répartition E/T/I/S/V renseignée pour cette séance.")
                continue
            total = sum(d.values())
            print("Répartition déclarée (hybride possible):")
            for k in ["E", "T", "I", "S", "V"]:
                v = d.get(k, 0.0)
                pct = (v / total * 100.0) if total > 0 else None
                print(f"  {k}: {fmt_hms(v)} ({fmt_float(pct,1)}%)")
            continue

        print("Choix invalide.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["principal", "advanced"], default="principal")
    parser.add_argument("--period", choices=["month", "year"], default="month")
    args = parser.parse_args()

    conn = connect()
    ensure_dashboard_tables(conn)

    if args.mode == "principal":
        dashboard_principal(conn, args.period)
    else:
        dashboard_advanced(conn)

    conn.close()


if __name__ == "__main__":
    main()
