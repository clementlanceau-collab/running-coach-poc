import argparse
import csv
import os
import sqlite3
from typing import Optional, Dict, Any, List, Tuple

from .db import ensure_dashboard_tables, table_exists, table_columns


DB_PATH = "running.db"


# -----------------------------
# Helpers
# -----------------------------
def to_int(x: str) -> Optional[int]:
    x = (x or "").strip()
    if x == "":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def to_float(x: str) -> Optional[float]:
    x = (x or "").strip()
    if x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def norm_bucket(b: str) -> Optional[str]:
    b = (b or "").strip().upper()
    if b in {"E", "T", "I", "S", "V"}:
        return b
    return None


def mkdir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def fetch_activity_rows(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name, sport_type
        FROM activities
        ORDER BY start_date_local DESC
        LIMIT ?;
    """, (limit,))
    return cur.fetchall()


# -----------------------------
# Read existing inputs (robust to schema drift)
# -----------------------------
def fetch_existing_inputs(conn: sqlite3.Connection, activity_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {aid: {} for aid in activity_ids}
    if not activity_ids:
        return out

    ids_sql = ",".join("?" for _ in activity_ids)
    cur = conn.cursor()

    # RPE
    if table_exists(conn, "session_rpe"):
        cols = set(table_columns(conn, "session_rpe"))
        select_cols = ["activity_id"]
        if "rpe" in cols:
            select_cols.append("rpe")
        if "note" in cols:
            select_cols.append("note")
        cur.execute(
            f"SELECT {', '.join(select_cols)} FROM session_rpe WHERE activity_id IN ({ids_sql});",
            tuple(activity_ids),
        )
        for r in cur.fetchall():
            aid = int(r["activity_id"])
            if "rpe" in cols:
                out[aid]["rpe"] = r["rpe"]
            if "note" in cols:
                out[aid]["rpe_note"] = r["note"]

    # Context
    if table_exists(conn, "session_context"):
        cols = set(table_columns(conn, "session_context"))
        select_cols = ["activity_id"]
        if "terrain_type" in cols:
            select_cols.append("terrain_type")
        if "shoes" in cols:
            select_cols.append("shoes")
        if "note" in cols:
            select_cols.append("note")
        cur.execute(
            f"SELECT {', '.join(select_cols)} FROM session_context WHERE activity_id IN ({ids_sql});",
            tuple(activity_ids),
        )
        for r in cur.fetchall():
            aid = int(r["activity_id"])
            if "terrain_type" in cols:
                out[aid]["terrain_type"] = r["terrain_type"]
            if "shoes" in cols:
                out[aid]["shoes"] = r["shoes"]
            if "note" in cols:
                out[aid]["context_note"] = r["note"]

    # Intensity tall -> pivot
    if table_exists(conn, "session_intensity"):
        cols = set(table_columns(conn, "session_intensity"))
        # We only handle tall schema here (activity_id, bucket, seconds)
        if {"activity_id", "bucket", "seconds"}.issubset(cols):
            cur.execute(f"""
                SELECT activity_id, bucket, SUM(seconds) AS seconds_sum
                FROM session_intensity
                WHERE activity_id IN ({ids_sql}) AND (source IS NULL OR source='DECLARED')
                GROUP BY activity_id, bucket;
            """, tuple(activity_ids))
            for r in cur.fetchall():
                aid = int(r["activity_id"])
                bucket = (r["bucket"] or "").upper()
                sec = r["seconds_sum"]
                if bucket in {"E", "T", "I", "S", "V"}:
                    out[aid][f"{bucket}_s"] = sec

    # Intensity note
    if table_exists(conn, "session_intensity_note"):
        cols = set(table_columns(conn, "session_intensity_note"))
        if "note" in cols:
            cur.execute(
                f"SELECT activity_id, note FROM session_intensity_note WHERE activity_id IN ({ids_sql});",
                tuple(activity_ids),
            )
            for r in cur.fetchall():
                out[int(r["activity_id"])]["intensity_note"] = r["note"]

    return out


# -----------------------------
# Upserts
# -----------------------------
def upsert_context(conn: sqlite3.Connection, activity_id: int,
                   terrain_type: Optional[str],
                   shoes: Optional[str],
                   note: Optional[str]) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO session_context(activity_id, terrain_type, shoes, note)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(activity_id) DO UPDATE SET
            terrain_type=excluded.terrain_type,
            shoes=excluded.shoes,
            note=excluded.note;
    """, (activity_id, terrain_type, shoes, note))


def upsert_rpe(conn: sqlite3.Connection, activity_id: int,
               rpe: Optional[int],
               note: Optional[str]) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO session_rpe(activity_id, rpe, note)
        VALUES (?, ?, ?)
        ON CONFLICT(activity_id) DO UPDATE SET
            rpe=excluded.rpe,
            note=excluded.note;
    """, (activity_id, rpe, note))


def upsert_intensity_note(conn: sqlite3.Connection, activity_id: int,
                          note: Optional[str]) -> None:
    if note is None or str(note).strip() == "":
        return
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO session_intensity_note(activity_id, note)
        VALUES (?, ?)
        ON CONFLICT(activity_id) DO UPDATE SET
            note=excluded.note;
    """, (activity_id, note))


def upsert_intensity_declared(conn: sqlite3.Connection, activity_id: int,
                              seconds_by_bucket: Dict[str, float]) -> None:
    cur = conn.cursor()
    for bucket, sec in seconds_by_bucket.items():
        b = norm_bucket(bucket)
        if b is None:
            continue
        if sec is None:
            continue
        try:
            sec_f = float(sec)
        except Exception:
            continue
        if sec_f <= 0:
            continue

        cur.execute("""
            INSERT INTO session_intensity(activity_id, bucket, seconds, source)
            VALUES (?, ?, ?, 'DECLARED')
            ON CONFLICT(activity_id, bucket) DO UPDATE SET
                seconds=excluded.seconds,
                source=excluded.source;
        """, (activity_id, b, sec_f))


# -----------------------------
# CSV export/import
# -----------------------------
CSV_COLUMNS = [
    "activity_id",
    "start_date_local",
    "name",
    "rpe",
    "rpe_note",
    "terrain_type",
    "shoes",
    "context_note",
    "E_s",
    "T_s",
    "I_s",
    "S_s",
    "V_s",
    "intensity_note",
]


def export_template(conn: sqlite3.Connection, out_path: str, limit: int) -> None:
    mkdir_for_file(out_path)

    activities = fetch_activity_rows(conn, limit)
    activity_ids = [int(r["activity_id"]) for r in activities]
    existing = fetch_existing_inputs(conn, activity_ids)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_COLUMNS)

        for r in activities:
            aid = int(r["activity_id"])
            ex = existing.get(aid, {})

            w.writerow([
                aid,
                r["start_date_local"] or "",
                r["name"] or "",
                ex.get("rpe", "") if ex.get("rpe") is not None else "",
                ex.get("rpe_note", "") if ex.get("rpe_note") is not None else "",
                ex.get("terrain_type", "") if ex.get("terrain_type") is not None else "",
                ex.get("shoes", "") if ex.get("shoes") is not None else "",
                ex.get("context_note", "") if ex.get("context_note") is not None else "",
                ex.get("E_s", "") if ex.get("E_s") is not None else "",
                ex.get("T_s", "") if ex.get("T_s") is not None else "",
                ex.get("I_s", "") if ex.get("I_s") is not None else "",
                ex.get("S_s", "") if ex.get("S_s") is not None else "",
                ex.get("V_s", "") if ex.get("V_s") is not None else "",
                ex.get("intensity_note", "") if ex.get("intensity_note") is not None else "",
            ])

    print(f"[OK] Template CSV généré: {out_path}")
    print("Colonnes:", ", ".join(CSV_COLUMNS))
    print(f"Lignes: {len(activities)} (hors header)")


def import_csv(conn: sqlite3.Connection, csv_path: str, strict: bool = False) -> None:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [c for c in CSV_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            msg = f"CSV invalide: colonnes manquantes: {missing}"
            if strict:
                raise ValueError(msg)
            print("[WARN]", msg)

        rows = list(reader)

    ok = 0
    skipped = 0
    errors: List[Tuple[int, str]] = []

    cur = conn.cursor()

    for i, row in enumerate(rows, start=2):
        try:
            activity_id = to_int(row.get("activity_id", ""))
            if activity_id is None:
                skipped += 1
                continue

            cur.execute("SELECT 1 FROM activities WHERE activity_id=?;", (activity_id,))
            if cur.fetchone() is None:
                msg = f"activity_id absent de activities: {activity_id}"
                if strict:
                    raise ValueError(msg)
                errors.append((i, msg))
                skipped += 1
                continue

            rpe = to_int(row.get("rpe", ""))
            rpe_note = (row.get("rpe_note", "") or "").strip() or None

            terrain_type = (row.get("terrain_type", "") or "").strip() or None
            shoes = (row.get("shoes", "") or "").strip() or None
            context_note = (row.get("context_note", "") or "").strip() or None

            E_s = to_float(row.get("E_s", ""))
            T_s = to_float(row.get("T_s", ""))
            I_s = to_float(row.get("I_s", ""))
            S_s = to_float(row.get("S_s", ""))
            V_s = to_float(row.get("V_s", ""))

            intensity_note = (row.get("intensity_note", "") or "").strip() or None

            upsert_rpe(conn, activity_id, rpe, rpe_note)
            upsert_context(conn, activity_id, terrain_type, shoes, context_note)

            seconds_by_bucket: Dict[str, float] = {}
            if E_s is not None:
                seconds_by_bucket["E"] = E_s
            if T_s is not None:
                seconds_by_bucket["T"] = T_s
            if I_s is not None:
                seconds_by_bucket["I"] = I_s
            if S_s is not None:
                seconds_by_bucket["S"] = S_s
            if V_s is not None:
                seconds_by_bucket["V"] = V_s

            if seconds_by_bucket:
                upsert_intensity_declared(conn, activity_id, seconds_by_bucket)

            upsert_intensity_note(conn, activity_id, intensity_note)

            ok += 1

        except Exception as e:
            if strict:
                raise
            errors.append((i, str(e)))
            skipped += 1

    conn.commit()

    print(f"[OK] Import terminé: ok={ok} | skipped={skipped} | errors={len(errors)}")
    if errors:
        print("Erreurs (premières 10):")
        for (line_no, msg) in errors[:10]:
            print(f"  - ligne {line_no}: {msg}")


# -----------------------------
# CLI
# -----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m app.dashboard.csv_tools")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_exp = sub.add_parser("export-template", help="Génère un CSV à remplir (inputs dashboard)")
    p_exp.add_argument("--out", required=True, help="Chemin du CSV à générer")
    p_exp.add_argument("--limit", type=int, default=30, help="Nombre de lignes (activités)")

    p_imp = sub.add_parser("import", help="Importe le CSV rempli vers la DB")
    p_imp.add_argument("csv_path", help="Chemin du CSV")
    p_imp.add_argument("--strict", action="store_true", help="Stoppe au premier problème")

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # IMPORTANT: assure + migre schéma dashboard (inclut ajout colonnes note si manquantes)
    ensure_dashboard_tables(conn)

    if args.cmd == "export-template":
        export_template(conn, args.out, args.limit)
    elif args.cmd == "import":
        import_csv(conn, args.csv_path, strict=bool(args.strict))

    conn.close()


if __name__ == "__main__":
    main()
