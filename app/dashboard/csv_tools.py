# app/dashboard/csv_tools.py
import argparse
import csv
import os
import sqlite3
from typing import Optional, Dict, List, Set, Tuple

DB_PATH = "running.db"

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


# ----------------------------
# Helpers parsing
# ----------------------------
def _to_int_or_none(x: str) -> Optional[int]:
    if x is None:
        return None
    x = str(x).strip()
    if x == "":
        return None
    try:
        return int(float(x))
    except ValueError:
        return None


def _to_float_or_none(x: str) -> Optional[float]:
    if x is None:
        return None
    x = str(x).strip()
    if x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def _to_str_or_none(x: str) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


# ----------------------------
# DB
# ----------------------------
def connect_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?;
        """,
        (table,),
    )
    return cur.fetchone() is not None


def existing_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    cols = set()
    for r in cur.fetchall():
        cols.add(r[1])  # name
    return cols


def ensure_table_with_migrations(conn: sqlite3.Connection) -> None:
    """
    Ensures session_rpe, session_context exist (simple).
    For session_intensity: if it exists, we DO NOT attempt to reshape it.
    We only ensure it exists if missing (using a sane default wide schema).
    Import logic will dynamically adapt to existing schema.
    """
    cur = conn.cursor()

    # session_rpe
    if not table_exists(conn, "session_rpe"):
        cur.execute(
            """
            CREATE TABLE session_rpe (
                activity_id INTEGER PRIMARY KEY,
                rpe REAL,
                rpe_note TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
    else:
        cols = existing_columns(conn, "session_rpe")
        if "rpe" not in cols:
            cur.execute("ALTER TABLE session_rpe ADD COLUMN rpe REAL;")
        if "rpe_note" not in cols:
            cur.execute("ALTER TABLE session_rpe ADD COLUMN rpe_note TEXT;")
        if "updated_at" not in cols:
            cur.execute("ALTER TABLE session_rpe ADD COLUMN updated_at TEXT;")

    # session_context
    if not table_exists(conn, "session_context"):
        cur.execute(
            """
            CREATE TABLE session_context (
                activity_id INTEGER PRIMARY KEY,
                terrain_type TEXT,
                shoes TEXT,
                context_note TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
    else:
        cols = existing_columns(conn, "session_context")
        if "terrain_type" not in cols:
            cur.execute("ALTER TABLE session_context ADD COLUMN terrain_type TEXT;")
        if "shoes" not in cols:
            cur.execute("ALTER TABLE session_context ADD COLUMN shoes TEXT;")
        if "context_note" not in cols:
            cur.execute("ALTER TABLE session_context ADD COLUMN context_note TEXT;")
        if "updated_at" not in cols:
            cur.execute("ALTER TABLE session_context ADD COLUMN updated_at TEXT;")

    # session_intensity
    # If it doesn't exist at all, create a simple wide schema (our dashboard V1 can read it).
    if not table_exists(conn, "session_intensity"):
        cur.execute(
            """
            CREATE TABLE session_intensity (
                activity_id INTEGER PRIMARY KEY,
                E_s INTEGER,
                T_s INTEGER,
                I_s INTEGER,
                S_s INTEGER,
                V_s INTEGER,
                intensity_note TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
            );
            """
        )

    conn.commit()


def activity_exists(conn: sqlite3.Connection, activity_id: int) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM activities WHERE activity_id = ? LIMIT 1;", (activity_id,))
    return cur.fetchone() is not None


# ----------------------------
# Intensity schema detection
# ----------------------------
def intensity_schema(conn: sqlite3.Connection) -> Tuple[str, Set[str]]:
    """
    Returns ("tall" | "wide", columns)
    tall = has columns like bucket + seconds (one row per bucket)
    wide = has columns like E_s, T_s, ... in the same row
    """
    cols = existing_columns(conn, "session_intensity")
    if "seconds" in cols and "bucket" in cols:
        return ("tall", cols)
    return ("wide", cols)


def _note_column(cols: Set[str]) -> Optional[str]:
    # Some schemas may use "note", others "intensity_note"
    if "intensity_note" in cols:
        return "intensity_note"
    if "note" in cols:
        return "note"
    return None


def _source_column(cols: Set[str]) -> Optional[str]:
    # optional; if present we can tag source
    if "source" in cols:
        return "source"
    return None


# ----------------------------
# Export template
# ----------------------------
def fetch_latest_activities(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT activity_id, start_date_local, name
        FROM activities
        ORDER BY start_date_local DESC
        LIMIT ?;
        """,
        (limit,),
    )
    return cur.fetchall()


def export_template(conn: sqlite3.Connection, out_path: str, limit: int) -> None:
    ensure_table_with_migrations(conn)

    rows = fetch_latest_activities(conn, limit)
    _ensure_parent_dir(out_path)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_COLUMNS)
        for r in rows:
            w.writerow(
                [
                    r["activity_id"],
                    r["start_date_local"],
                    r["name"],
                    "",  # rpe
                    "",  # rpe_note
                    "",  # terrain_type
                    "",  # shoes
                    "",  # context_note
                    "",  # E_s
                    "",  # T_s
                    "",  # I_s
                    "",  # S_s
                    "",  # V_s
                    "",  # intensity_note
                ]
            )

    print(f"[OK] Template CSV généré: {out_path}")
    print("Colonnes: " + ", ".join(CSV_COLUMNS))
    print(f"Lignes: {len(rows)} (hors header)")


# ----------------------------
# Import CSV -> DB (UPSERT)
# ----------------------------
def read_csv_rows(csv_path: str) -> List[Dict[str, str]]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV introuvable: {csv_path}")

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV invalide: pas de header.")

        missing = [c for c in CSV_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"CSV invalide: colonnes manquantes: {missing}")

        return list(reader)


def upsert_rpe(conn: sqlite3.Connection, activity_id: int, rpe: Optional[float], rpe_note: Optional[str]) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE session_rpe
        SET rpe = ?, rpe_note = ?, updated_at = datetime('now')
        WHERE activity_id = ?;
        """,
        (rpe, rpe_note, activity_id),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO session_rpe (activity_id, rpe, rpe_note, updated_at)
            VALUES (?, ?, ?, datetime('now'));
            """,
            (activity_id, rpe, rpe_note),
        )


def upsert_context(
    conn: sqlite3.Connection,
    activity_id: int,
    terrain_type: Optional[str],
    shoes: Optional[str],
    context_note: Optional[str],
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE session_context
        SET terrain_type = ?, shoes = ?, context_note = ?, updated_at = datetime('now')
        WHERE activity_id = ?;
        """,
        (terrain_type, shoes, context_note, activity_id),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO session_context (activity_id, terrain_type, shoes, context_note, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'));
            """,
            (activity_id, terrain_type, shoes, context_note),
        )


def upsert_intensity_wide(
    conn: sqlite3.Connection,
    cols: Set[str],
    activity_id: int,
    E_s: Optional[int],
    T_s: Optional[int],
    I_s: Optional[int],
    S_s: Optional[int],
    V_s: Optional[int],
    intensity_note: Optional[str],
) -> None:
    note_col = _note_column(cols)

    # Only write columns that exist (legacy-friendly)
    set_parts = []
    params: List[object] = []

    for colname, val in [("E_s", E_s), ("T_s", T_s), ("I_s", I_s), ("S_s", S_s), ("V_s", V_s)]:
        if colname in cols:
            set_parts.append(f"{colname} = ?")
            params.append(val)

    if note_col is not None:
        set_parts.append(f"{note_col} = ?")
        params.append(intensity_note)

    if "updated_at" in cols:
        set_parts.append("updated_at = datetime('now')")

    # UPDATE then INSERT
    cur = conn.cursor()
    if set_parts:
        cur.execute(
            f"""
            UPDATE session_intensity
            SET {", ".join(set_parts)}
            WHERE activity_id = ?;
            """,
            params + [activity_id],
        )

    if cur.rowcount == 0:
        insert_cols = ["activity_id"]
        insert_vals = [activity_id]

        for colname, val in [("E_s", E_s), ("T_s", T_s), ("I_s", I_s), ("S_s", S_s), ("V_s", V_s)]:
            if colname in cols:
                insert_cols.append(colname)
                insert_vals.append(val)

        if note_col is not None:
            insert_cols.append(note_col)
            insert_vals.append(intensity_note)

        if "updated_at" in cols:
            insert_cols.append("updated_at")

        placeholders = ", ".join(["?"] * len(insert_vals)) + (", datetime('now')" if "updated_at" in cols else "")
        cur.execute(
            f"""
            INSERT INTO session_intensity ({", ".join(insert_cols)})
            VALUES ({placeholders});
            """,
            insert_vals,
        )


def upsert_intensity_tall(
    conn: sqlite3.Connection,
    cols: Set[str],
    activity_id: int,
    E_s: Optional[int],
    T_s: Optional[int],
    I_s: Optional[int],
    S_s: Optional[int],
    V_s: Optional[int],
    intensity_note: Optional[str],
) -> None:
    """
    Tall schema: one row per bucket with seconds NOT NULL.
    We upsert (activity_id, bucket) rows for any bucket that has a value.
    """
    note_col = _note_column(cols)
    source_col = _source_column(cols)

    # mapping buckets -> seconds
    bucket_map = {
        "E": E_s,
        "T": T_s,
        "I": I_s,
        "S": S_s,
        "V": V_s,
    }

    cur = conn.cursor()

    # Determine whether there is a UNIQUE/PK on (activity_id,bucket).
    # We don't rely on ON CONFLICT; we do UPDATE then INSERT.
    for bucket, seconds in bucket_map.items():
        if seconds is None:
            continue  # no write
        # UPDATE
        set_parts = ["seconds = ?"]
        params: List[object] = [seconds]

        if note_col is not None:
            set_parts.append(f"{note_col} = ?")
            params.append(intensity_note)

        if source_col is not None:
            set_parts.append(f"{source_col} = ?")
            params.append("CSV")

        if "updated_at" in cols:
            set_parts.append("updated_at = datetime('now')")

        cur.execute(
            f"""
            UPDATE session_intensity
            SET {", ".join(set_parts)}
            WHERE activity_id = ? AND bucket = ?;
            """,
            params + [activity_id, bucket],
        )

        if cur.rowcount == 0:
            insert_cols = ["activity_id", "bucket", "seconds"]
            insert_vals: List[object] = [activity_id, bucket, seconds]

            if note_col is not None:
                insert_cols.append(note_col)
                insert_vals.append(intensity_note)

            if source_col is not None:
                insert_cols.append(source_col)
                insert_vals.append("CSV")

            if "updated_at" in cols:
                insert_cols.append("updated_at")

            placeholders = ", ".join(["?"] * len(insert_vals)) + (", datetime('now')" if "updated_at" in cols else "")
            cur.execute(
                f"""
                INSERT INTO session_intensity ({", ".join(insert_cols)})
                VALUES ({placeholders});
                """,
                insert_vals,
            )


def upsert_intensity(
    conn: sqlite3.Connection,
    activity_id: int,
    E_s: Optional[int],
    T_s: Optional[int],
    I_s: Optional[int],
    S_s: Optional[int],
    V_s: Optional[int],
    intensity_note: Optional[str],
) -> None:
    schema, cols = intensity_schema(conn)
    if schema == "tall":
        upsert_intensity_tall(conn, cols, activity_id, E_s, T_s, I_s, S_s, V_s, intensity_note)
    else:
        upsert_intensity_wide(conn, cols, activity_id, E_s, T_s, I_s, S_s, V_s, intensity_note)


def import_csv(conn: sqlite3.Connection, csv_path: str, strict: bool = False) -> None:
    ensure_table_with_migrations(conn)

    rows = read_csv_rows(csv_path)

    n_total = 0
    n_upsert = 0
    n_ignored = 0
    warnings: List[str] = []

    for row in rows:
        n_total += 1

        activity_id = _to_int_or_none(row.get("activity_id", ""))
        if activity_id is None:
            msg = f"Ligne {n_total}: activity_id invalide."
            if strict:
                raise ValueError(msg)
            warnings.append(msg)
            n_ignored += 1
            continue

        if not activity_exists(conn, activity_id):
            msg = f"Ligne {n_total}: activity_id {activity_id} absent de activities (skip)."
            if strict:
                raise ValueError(msg)
            warnings.append(msg)
            n_ignored += 1
            continue

        rpe = _to_float_or_none(row.get("rpe", ""))
        rpe_note = _to_str_or_none(row.get("rpe_note", ""))

        terrain_type = _to_str_or_none(row.get("terrain_type", ""))
        shoes = _to_str_or_none(row.get("shoes", ""))
        context_note = _to_str_or_none(row.get("context_note", ""))

        E_s = _to_int_or_none(row.get("E_s", ""))
        T_s = _to_int_or_none(row.get("T_s", ""))
        I_s = _to_int_or_none(row.get("I_s", ""))
        S_s = _to_int_or_none(row.get("S_s", ""))
        V_s = _to_int_or_none(row.get("V_s", ""))
        intensity_note = _to_str_or_none(row.get("intensity_note", ""))

        wrote_any = False

        if rpe is not None or rpe_note is not None:
            upsert_rpe(conn, activity_id, rpe, rpe_note)
            wrote_any = True

        if terrain_type is not None or shoes is not None or context_note is not None:
            upsert_context(conn, activity_id, terrain_type, shoes, context_note)
            wrote_any = True

        if (
            E_s is not None
            or T_s is not None
            or I_s is not None
            or S_s is not None
            or V_s is not None
            or intensity_note is not None
        ):
            upsert_intensity(conn, activity_id, E_s, T_s, I_s, S_s, V_s, intensity_note)
            wrote_any = True

        if wrote_any:
            n_upsert += 1
        else:
            n_ignored += 1

    conn.commit()

    schema, cols = intensity_schema(conn)
    print(f"[OK] Import terminé: {csv_path}")
    print(f"  lignes total    : {n_total}")
    print(f"  lignes upsert   : {n_upsert}")
    print(f"  lignes ignorées : {n_ignored}")
    print(f"  session_intensity schema détecté: {schema} (cols: {', '.join(sorted(list(cols)))})")

    if warnings:
        print("  warnings:")
        for w in warnings[:20]:
            print("   - " + w)
        if len(warnings) > 20:
            print(f"   - ... ({len(warnings) - 20} autres)")


# ----------------------------
# CLI
# ----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m app.dashboard.csv_tools")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_export = sub.add_parser("export-template", help="Génère un CSV template à remplir.")
    p_export.add_argument("--out", required=True, help="Chemin du CSV de sortie.")
    p_export.add_argument("--limit", type=int, default=30, help="Nombre d'activités à inclure.")

    p_import = sub.add_parser("import", help="Importe le CSV et upsert dans la DB.")
    p_import.add_argument("csv_path", help="Chemin du CSV rempli.")
    p_import.add_argument("--strict", action="store_true", help="Erreur bloquante si données invalides.")

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    conn = connect_db(DB_PATH)
    try:
        if args.cmd == "export-template":
            export_template(conn, args.out, args.limit)
        elif args.cmd == "import":
            import_csv(conn, args.csv_path, strict=bool(args.strict))
        else:
            raise ValueError("Commande inconnue.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
