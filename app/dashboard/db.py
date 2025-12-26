import sqlite3
from typing import Optional, Iterable, Any, List, Tuple, Dict

DB_PATH = "running.db"


def connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_dashboard_tables(conn: sqlite3.Connection) -> None:
    """
    Tables *optionnelles* nécessaires au Dashboard V1 (section 18) :
    - activity_context : terrain / notes factuelles (déclaré)
    - session_rpe      : RPE (déclaré)
    - session_intensity: répartition E/T/I/S/V (déclaré), hybride autorisé (plusieurs catégories)
    Ces tables n'interfèrent pas avec le socle Strava; elles complètent.
    """
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS activity_context (
        activity_id INTEGER PRIMARY KEY,
        terrain_type TEXT NULL,            -- 'route'|'trail'|'piste'|'unknown' (déclaré)
        surface_note TEXT NULL,            -- texte libre factuel (ex: "route humide", "piste")
        shoes TEXT NULL,                   -- optionnel
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_intensity (
        activity_id INTEGER NOT NULL,
        bucket TEXT NOT NULL,              -- 'E'|'T'|'I'|'S'|'V'
        seconds REAL NOT NULL CHECK(seconds >= 0),
        source TEXT NOT NULL DEFAULT 'DECLARED',
        PRIMARY KEY (activity_id, bucket)
    );
    """)

    conn.commit()


def q(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()
