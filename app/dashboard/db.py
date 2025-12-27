import sqlite3
from pathlib import Path

DB_PATH = Path("running.db")


def connect_db():
    """
    Open a connection to the running SQLite database.
    """
    return sqlite3.connect(DB_PATH)


def q(conn, sql, params=()):
    """
    Execute a query and return all rows.
    """
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def _table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    cols = [r[1] for r in cur.fetchall()]  # r[1] = name
    return column_name in cols


def _ensure_column(conn, table_name: str, column_name: str, column_sql_type: str):
    """
    If table exists and column is missing, add it via ALTER TABLE.
    SQLite supports ADD COLUMN (with limited constraints).
    """
    if _table_exists(conn, table_name) and not _column_exists(conn, table_name, column_name):
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql_type}")
        conn.commit()


def ensure_dashboard_tables(conn):
    """
    Create dashboard tables if missing, and migrate (add) missing columns safely.
    This function is designed to be idempotent.
    """
    cur = conn.cursor()

    # --- session_context
    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_context (
        activity_id INTEGER PRIMARY KEY,
        terrain_type TEXT,
        shoes TEXT,
        context_note TEXT
    )
    """)

    # If the table existed from older versions, ensure columns exist
    _ensure_column(conn, "session_context", "terrain_type", "TEXT")
    _ensure_column(conn, "session_context", "shoes", "TEXT")
    _ensure_column(conn, "session_context", "context_note", "TEXT")

    # --- session_rpe
    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_rpe (
        activity_id INTEGER PRIMARY KEY,
        rpe INTEGER,
        rpe_note TEXT
    )
    """)

    _ensure_column(conn, "session_rpe", "rpe", "INTEGER")
    _ensure_column(conn, "session_rpe", "rpe_note", "TEXT")

    # --- session_intensity (tall schema)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_intensity (
        activity_id INTEGER,
        bucket TEXT,
        seconds REAL,
        source TEXT DEFAULT 'DECLARED',
        PRIMARY KEY (activity_id, bucket)
    )
    """)

    _ensure_column(conn, "session_intensity", "bucket", "TEXT")
    _ensure_column(conn, "session_intensity", "seconds", "REAL")
    _ensure_column(conn, "session_intensity", "source", "TEXT")

    # --- session_intensity_note
    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_intensity_note (
        activity_id INTEGER PRIMARY KEY,
        intensity_note TEXT
    )
    """)

    _ensure_column(conn, "session_intensity_note", "intensity_note", "TEXT")

    conn.commit()
