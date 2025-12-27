import sqlite3
from typing import Any, Tuple, List


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    )
    return cur.fetchone() is not None


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]  # r[1] = name


def table_pk_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    """
    Returns PK columns ordered by pk index.
    PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    pk is 0 if not part of PK, else 1..n indicates order.
    """
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    rows = cur.fetchall()
    pk_cols = [(r[5], r[1]) for r in rows if r[5] and int(r[5]) > 0]
    pk_cols.sort(key=lambda x: x[0])
    return [name for _, name in pk_cols]


def ensure_column(conn: sqlite3.Connection, table: str, col: str, col_def: str) -> None:
    """
    Adds a column if missing. col_def example: "note TEXT NULL"
    """
    if not table_exists(conn, table):
        return
    cols = set(table_columns(conn, table))
    if col in cols:
        return
    cur = conn.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def};")


def ensure_dashboard_tables(conn: sqlite3.Connection) -> None:
    """
    Dashboard V1 storage.
    - session_context: terrain/shoes/context note
    - session_rpe: rpe + note
    - session_intensity: declared E/T/I/S/V as tall schema (hybrid allowed)
    - session_intensity_note: optional free text note about declared split
    """

    cur = conn.cursor()

    # Create tables if missing
    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_context (
        activity_id INTEGER PRIMARY KEY,
        terrain_type TEXT NULL,
        shoes TEXT NULL,
        note TEXT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_rpe (
        activity_id INTEGER PRIMARY KEY,
        rpe INTEGER NULL CHECK (rpe BETWEEN 1 AND 10),
        note TEXT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)

    # Migrate intensity schema if needed
    migrate_session_intensity(conn)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_intensity_note (
        activity_id INTEGER PRIMARY KEY,
        note TEXT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)

    # If tables pre-existed with missing columns, add them now
    ensure_column(conn, "session_context", "terrain_type", "terrain_type TEXT NULL")
    ensure_column(conn, "session_context", "shoes", "shoes TEXT NULL")
    ensure_column(conn, "session_context", "note", "note TEXT NULL")

    ensure_column(conn, "session_rpe", "rpe", "rpe INTEGER NULL")
    ensure_column(conn, "session_rpe", "note", "note TEXT NULL")

    ensure_column(conn, "session_intensity_note", "note", "note TEXT NULL")

    conn.commit()


def migrate_session_intensity(conn: sqlite3.Connection) -> None:
    """
    Goal schema (tall):
      session_intensity(
        activity_id INTEGER NOT NULL,
        bucket TEXT NOT NULL,     -- 'E'|'T'|'I'|'S'|'V'
        seconds REAL NOT NULL CHECK(seconds >= 0),
        source TEXT NOT NULL DEFAULT 'DECLARED',
        PRIMARY KEY (activity_id, bucket)
      )

    If existing is:
    - missing -> create
    - wide (E_s, T_s...) -> convert
    - tall but wrong unique/pk -> rebuild
    """
    cur = conn.cursor()

    if not table_exists(conn, "session_intensity"):
        cur.execute("""
        CREATE TABLE session_intensity (
            activity_id INTEGER NOT NULL,
            bucket TEXT NOT NULL,
            seconds REAL NOT NULL CHECK(seconds >= 0),
            source TEXT NOT NULL DEFAULT 'DECLARED',
            PRIMARY KEY (activity_id, bucket)
        );
        """)
        conn.commit()
        return

    cols = table_columns(conn, "session_intensity")
    pk_cols = table_pk_columns(conn, "session_intensity")

    wide_markers = {"E_s", "T_s", "I_s", "S_s", "V_s"}
    is_wide = any(c in cols for c in wide_markers)

    has_tall_cols = ("activity_id" in cols) and ("bucket" in cols) and ("seconds" in cols)

    if has_tall_cols and pk_cols == ["activity_id", "bucket"]:
        # Ensure source column exists (some earlier versions may have no source)
        ensure_column(conn, "session_intensity", "source", "source TEXT NOT NULL DEFAULT 'DECLARED'")
        conn.commit()
        return

    # Need rebuild to correct tall schema
    cur.execute("BEGIN;")
    try:
        cur.execute("ALTER TABLE session_intensity RENAME TO session_intensity_old;")

        cur.execute("""
        CREATE TABLE session_intensity (
            activity_id INTEGER NOT NULL,
            bucket TEXT NOT NULL,
            seconds REAL NOT NULL CHECK(seconds >= 0),
            source TEXT NOT NULL DEFAULT 'DECLARED',
            PRIMARY KEY (activity_id, bucket)
        );
        """)

        old_cols = table_columns(conn, "session_intensity_old")

        if is_wide:
            for bucket in ["E", "T", "I", "S", "V"]:
                col = f"{bucket}_s"
                if col in old_cols:
                    cur.execute(f"""
                        INSERT INTO session_intensity(activity_id, bucket, seconds, source)
                        SELECT activity_id, '{bucket}', CAST({col} AS REAL), 'DECLARED'
                        FROM session_intensity_old
                        WHERE {col} IS NOT NULL AND CAST({col} AS REAL) > 0;
                    """)

        elif has_tall_cols:
            if "source" in old_cols:
                cur.execute("""
                    INSERT INTO session_intensity(activity_id, bucket, seconds, source)
                    SELECT activity_id, bucket, CAST(seconds AS REAL), COALESCE(source, 'DECLARED')
                    FROM session_intensity_old
                    WHERE bucket IS NOT NULL AND seconds IS NOT NULL;
                """)
            else:
                cur.execute("""
                    INSERT INTO session_intensity(activity_id, bucket, seconds, source)
                    SELECT activity_id, bucket, CAST(seconds AS REAL), 'DECLARED'
                    FROM session_intensity_old
                    WHERE bucket IS NOT NULL AND seconds IS NOT NULL;
                """)

        # Keep old backup only if conversion not possible; otherwise drop
        cur.execute("DROP TABLE session_intensity_old;")

        conn.commit()
    except Exception:
        conn.rollback()
        raise


def q(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()
