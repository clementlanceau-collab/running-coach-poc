import sqlite3
from typing import List, Tuple, Dict, Any, Optional


DB_PATH = "running.db"


def ensure_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_signatures (
        activity_id INTEGER PRIMARY KEY,
        signature TEXT NOT NULL,
        signature_compact TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_session_signatures_compact ON session_signatures(signature_compact);")
    conn.commit()


def list_recent_runs(conn: sqlite3.Connection, limit: int = 30) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name
        FROM activities
        WHERE sport_type IN ('Run','Trail Run') AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT ?;
    """, (limit,))
    return cur.fetchall()


def fetch_tag_summary(conn: sqlite3.Connection, activity_id: int) -> List[Tuple]:
    """
    Retourne par tag:
      tag, block, n_laps, total_s, total_m
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            t.tag,
            COALESCE(t.block, 'UNSPEC') AS block,
            COUNT(*) AS n_laps,
            SUM(l.elapsed_time_s) AS total_s,
            SUM(l.distance_m) AS total_m
        FROM lap_tags t
        JOIN laps_strava l
          ON l.activity_id = t.activity_id AND l.lap_index = t.lap_index
        WHERE t.activity_id = ? AND t.source='STRAVA_LAP'
        GROUP BY t.tag, COALESCE(t.block, 'UNSPEC')
        ORDER BY
            CASE COALESCE(t.block,'UNSPEC')
                WHEN 'WARMUP' THEN 1
                WHEN 'MAIN' THEN 2
                WHEN 'COOLDOWN' THEN 3
                ELSE 9
            END,
            total_s DESC,
            t.tag ASC;
    """, (activity_id,))
    return cur.fetchall()


def build_signature(rows: List[Tuple]) -> Tuple[str, str]:
    """
    Signature lisible + signature compacte (pour comparaison).
    - lisible: garde bloc + tag + volumes
    - compacte: garde bloc + tag uniquement, ordonnés de manière stable
    """
    # rows: (tag, block, n_laps, total_s, total_m)

    by_block: Dict[str, List[Tuple]] = {}
    for (tag, block, n_laps, total_s, total_m) in rows:
        by_block.setdefault(block, []).append((tag, int(n_laps), int(total_s or 0), float(total_m or 0.0)))

    block_order = ["WARMUP", "MAIN", "COOLDOWN", "UNSPEC"]
    parts_readable = []
    parts_compact = []

    for b in block_order:
        if b not in by_block:
            continue

        # tri stable: par durée desc puis tag
        items = sorted(by_block[b], key=lambda x: (-x[2], x[0]))

        readable_items = []
        compact_items = []
        for (tag, n_laps, total_s, total_m) in items:
            readable_items.append(f"{tag}({n_laps} laps, {total_s}s, {total_m:.0f}m)")
            compact_items.append(f"{tag}:{n_laps}")

        parts_readable.append(f"{b}: " + " + ".join(readable_items))
        parts_compact.append(f"{b}: " + " + ".join(compact_items))

    signature = " | ".join(parts_readable) if parts_readable else "NO_TAGS"
    signature_compact = " | ".join(parts_compact) if parts_compact else "NO_TAGS"
    return signature, signature_compact


def upsert_signature(conn: sqlite3.Connection, activity_id: int, signature: str, signature_compact: str) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO session_signatures(activity_id, signature, signature_compact)
        VALUES (?, ?, ?)
        ON CONFLICT(activity_id)
        DO UPDATE SET
            signature=excluded.signature,
            signature_compact=excluded.signature_compact,
            created_at=datetime('now');
    """, (activity_id, signature, signature_compact))
    conn.commit()


def find_same_signature(conn: sqlite3.Connection, signature_compact: str, limit: int = 20) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT s.activity_id, a.start_date_local, a.name
        FROM session_signatures s
        JOIN activities a ON a.activity_id = s.activity_id
        WHERE s.signature_compact = ?
        ORDER BY a.start_date_local DESC
        LIMIT ?;
    """, (signature_compact, limit))
    return cur.fetchall()


def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    print("\nDernières activités RUN/TRAIL (streams OK):")
    for (aid, dt, name) in list_recent_runs(conn, 20):
        print(f"{aid} | {dt} | {name}")

    activity_id = int(input("\nactivity_id = ").strip())

    rows = fetch_tag_summary(conn, activity_id)
    sig, sig_c = build_signature(rows)

    upsert_signature(conn, activity_id, sig, sig_c)

    print("\nSignature (lisible):")
    print(sig)
    print("\nSignature (compacte):")
    print(sig_c)

    same = find_same_signature(conn, sig_c, limit=20)
    print("\nSéances avec la même signature (compacte):")
    for r in same:
        print(r)

    conn.close()


if __name__ == "__main__":
    main()
