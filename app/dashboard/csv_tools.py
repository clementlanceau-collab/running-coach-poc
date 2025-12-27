import argparse
import csv
import os
import sqlite3
from typing import Optional, Dict, Any, List, Tuple

from .db import ensure_dashboard_tables, table_exists, table_columns

DB_PATH = "running.db"


# -----------------------------
# Helpers (parsing + normalization)
# -----------------------------
def mkdir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def to_int(x: str) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def to_float(x: str) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def norm_bucket(b: str) -> Optional[str]:
    if not b:
        return None
    s = str(b).strip().upper()
    if s in {"E", "T", "I", "S", "V"}:
        return s
    return None


def pick_note_column(conn: sqlite3.Connection, table_name: str, preferred: str, legacy: str) -> Optional[str]:
    """
    Returns which column to use for notes:
    - preferred (new schema) if exists
    - else legacy (old schema) if exists
    - else None
    """
    if not table_exists(conn, table_name):
        return None
    cols = set(table_columns(conn, table_name))
    if preferred in cols:
        return preferred
    if legacy in cols:
        return legacy
    return None


# -----------------------------
# Fetch activities (source data)
# -----------------------------
def fetch_activity_rows(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT activity_id, start_date_local, name, type
        FROM activities
        ORDER BY start_date_local DESC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


# -----------------------------
# Read existing dashboard inputs for export-template
# -----------------------------
def fetch_existing_inputs(conn: sqlite3.Connection, activity_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Build a dict keyed by activity_id with existing manual inputs:
    - session_rpe: rpe, rpe_note (or legacy note)
    - session_context: terrain_type, shoes, context_note (or legacy note)
    - session_intensity: tall schema -> E/T/I/S/V seconds
    - session_intensity_note: intensity_note (or legacy note)
    """
    out: Dict[int, Dict[str, Any]] = {aid: {} for aid in activity_ids}
    if not activity_ids:
        return out

    conn.row_factory = sqlite3.Row
    ids_sql = ",".join("?" for _ in activity_ids)
    cur = conn.cursor()

    # RPE
    if table_exists(conn, "session_rpe"):
        cols = set(table_columns(conn, "session_rpe"))
        rpe_note_col = pick_note_column(conn, "session_rpe", "rpe_note", "note")
        select_cols = ["activity_id"]
        if "rpe" in cols:
            select_cols.append("rpe")
        if rpe_note_col:
            select_cols.append(rpe_note_col)

        cur.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM session_rpe
            WHERE activity_id IN ({ids_sql})
            """,
            tuple(activity_ids),
        )
        for r in cur.fetchall():
            aid = int(r["activity_id"])
            if "rpe" in r.keys():
                out[aid]["rpe"] = r["rpe"]
            if rpe_note_col and rpe_note_col in r.keys():
                out[aid]["rpe_note"] = r[rpe_note_col]

    # Context
    if table_exists(conn, "session_context"):
        cols = set(table_columns(conn, "session_context"))
        ctx_note_col = pick_note_column(conn, "session_context", "context_note", "note")
        select_cols = ["activity_id"]
        if "terrain_type" in cols:
            select_cols.append("terrain_type")
        if "shoes" in cols:
            select_cols.append("shoes")
        if ctx_note_col:
            select_cols.append(ctx_note_col)

        cur.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM session_context
            WHERE activity_id IN ({ids_sql})
            """,
            tuple(activity_ids),
        )
        for r in cur.fetchall():
            aid = int(r["activity_id"])
            if "terrain_type" in r.keys():
                out[aid]["terrain_type"] = r["terrain_type"]
            if "shoes" in r.keys():
                out[aid]["shoes"] = r["shoes"]
            if ctx_note_col and ctx_note_col in r.keys():
                out[aid]["context_note"] = r[ctx_note_col]

    # Intensity note
    if table_exists(conn, "session_intensity_note"):
        inten_note_col = pick_note_column(conn, "session_intensity_note", "intensity_note", "note")
        if inten_note_col:
            cur.execute(
                f"""
                SELECT activity_id, {inten_note_col} AS note_value
                FROM session_intensity_note
                WHERE activity_id IN ({ids_sql})
                """,
                tuple(activity_ids),
            )
            for r in cur.fetchall():
                aid = int(r["activity_id"])
                out[aid]["intensity_note"] = r["note_value"]

    # Intensity tall -> pivot
    if table_exists(conn, "session_intensity"):
        cols = set(table_columns(conn, "session_intensity"))
        if {"activity_id", "bucket", "seconds"}.issubset(cols):
            # Only declared (or NULL) to avoid future extensions
            if "source" in cols:
                where_source = "AND (source IS NULL OR source='DECLARED')"
            else:
                where_source = ""

            cur.execute(
                f"""
                SELECT activity_id, bucket, SUM(seconds) AS seconds_sum
                FROM session_intensity
                WHERE activity_id IN ({ids_sql}) {where_source}
                GROUP BY activity_id, bucket
                """,
                tuple(activity_ids),
            )
            for r in cur.fetchall():
                aid = int(r["activity_id"])
                bucket = norm_bucket(r["bucket"] or "")
                if not bucket:
                    continue
                sec = r["seconds_sum"]
                out[aid][f"{bucket}_s"] = sec

    return out


# -----------------------------
# Upserts (write manual inputs)
# -----------------------------
def upsert_context(conn: sqlite3.Connection, activity_id: int,
                   terrain_type: Optional[str],
                   shoes: Optional[str],
                   context_note: Optional[str]) -> None:
    cols = set(table_columns(conn, "session_context"))
    note_col = "context_note" if "context_note" in cols else ("note" if "note" in cols else None)

    if note_col:
        conn.execute(
            f"""
            INSERT INTO session_context(activity_id, terrain_type, shoes, {note_col})
            VALUES (?, ?, ?, ?)
            ON CONFLICT(activity_id) DO UPDATE SET
                terrain_type=excluded.terrain_type,
                shoes=excluded.shoes,
                {note_col}=excluded.{note_col}
            """,
            (activity_id, terrain_type, shoes, context_note),
        )
    else:
        conn.execute(
            """
            INSERT INTO session_context(activity_id, terrain_type, shoes)
            VALUES (?, ?, ?)
            ON CONFLICT(activity_id) DO UPDATE SET
                terrain_type=excluded.terrain_type,
                shoes=excluded.shoes
            """,
            (activity_id, terrain_type, shoes),
        )


def upsert_rpe(conn: sqlite3.Connection, activity_id: int,
               rpe: Optional[int],
               rpe_note: Optional[str]) -> None:
    cols = set(table_columns(conn, "session_rpe"))
    note_col = "rpe_note" if "rpe_note" in cols else ("note" if "note" in cols else None)

    if note_col:
        conn.execute(
            f"""
            INSERT INTO session_rpe(activity_id, rpe, {note_col})
            VALUES (?, ?, ?)
            ON CONFLICT(activity_id) DO UPDATE SET
                rpe=excluded.rpe,
                {note_col}=excluded.{note_col}
            """,
            (activity_id, rpe, rpe_note),
        )
    else:
        conn.execute(
            """
            INSERT INTO session_rpe(activity_id, rpe)
            VALUES (?, ?)
            ON CONFLICT(activity_id) DO UPDATE SET
                rpe=excluded.rpe
            """,
            (activity_id, rpe),
        )


def upsert_intensity_note(conn: sqlite3.Connection, activity_id: int,
                          intensity_note: Optional[str]) -> None:
    # keep behavior: do not write empty strings
    if intensity_note is not None and str(intensity_note).strip() == "":
        intensity_note = None

    cols = set(table_columns(conn, "session_intensity_note"))
    note_col = "intensity_note" if "intensity_note" in cols else ("note" if "note" in cols else None)

    if note_col is None:
        return

    conn.execute(
        f"""
        INSERT INTO session_intensity_note(activity_id, {note_col})
        VALUES (?, ?)
        ON CONFLICT(activity_id) DO UPDATE SET
            {note_col}=excluded.{note_col}
        """,
        (activity_id, intensity_note),
    )


def upsert_intensity_declared(conn: sqlite3.Connection, activity_id: int,
                              seconds_by_bucket: Dict[str, float]) -> None:
    cols = set(table_columns(conn, "session_intensity"))
    has_source = "source" in cols

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

        if has_source:
            conn.execute(
                """
                INSERT INTO session_intensity(activity_id, bucket, seconds, source)
                VALUES (?, ?, ?, 'DECLARED')
                ON CONFLICT(activity_id, bucket) DO UPDATE SET
                    seconds=excluded.seconds,
                    source=excluded.source
                """,
                (activity_id, b, sec_f),
            )
        else:
            conn.execute(
                """
                INSERT INTO session_intensity(activity_id, bucket, seconds)
                VALUES (?, ?, ?)
                ON CONFLICT(activity_id, bucket) DO UPDATE SET
                    seconds=excluded.seconds
                """,
                (activity_id, b, sec_f),
            )


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

    print("[OK] Template CSV généré:", out_path)
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
    errors = 0

    for row in rows:
        try:
            activity_id = to_int(row.get("activity_id", ""))
            if activity_id is None:
                skipped += 1
                continue

            # Parse
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

            # Write
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

            # IMPORTANT: write intensity_note even if None (will upsert NULL)
            upsert_intensity_note(conn, activity_id, intensity_note)

            ok += 1

        except Exception as e:
            errors += 1
            if strict:
                raise
            print(f"[ERR] activity_id={row.get('activity_id')} -> {e}")

    conn.commit()
    print(f"[OK] Import terminé: ok={ok} | skipped={skipped} | errors={errors}")


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


def main():
    parser = build_parser()
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Ensure tables + migrations
    ensure_dashboard_tables(conn)

    if args.cmd == "export-template":
        export_template(conn, args.out, args.limit)
    elif args.cmd == "import":
        import_csv(conn, args.csv_path, strict=bool(args.strict))

    conn.close()


if __name__ == "__main__":
    main()
