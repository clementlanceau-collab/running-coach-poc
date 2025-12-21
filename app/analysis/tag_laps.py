import sqlite3
from typing import List, Tuple, Optional, Dict, Any


DB_PATH = "running.db"


def ensure_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lap_tags (
        activity_id INTEGER NOT NULL,
        source TEXT NOT NULL,              -- 'STRAVA_LAP'
        lap_index INTEGER NOT NULL,
        tag TEXT NOT NULL,                 -- ex: 'strides_3x15', 'set_3x3', 'set_4x90', 'set_4x30'
        block TEXT,                        -- ex: 'WARMUP', 'MAIN', 'COOLDOWN' (optionnel)
        note TEXT,                         -- texte libre
        PRIMARY KEY (activity_id, source, lap_index)
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lap_tags_act ON lap_tags(activity_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lap_tags_tag ON lap_tags(tag);")
    conn.commit()


def tag_strava_laps(conn: sqlite3.Connection,
                    activity_id: int,
                    lap_indexes: List[int],
                    tag: str,
                    block: Optional[str] = None,
                    note: Optional[str] = None) -> int:
    """
    Tag simple et idempotent:
    - si un lap est déjà taggé, on remplace le tag/block/note.
    """
    cur = conn.cursor()
    rows = []
    for lap_i in lap_indexes:
        rows.append((activity_id, "STRAVA_LAP", int(lap_i), tag, block, note))

    cur.executemany("""
        INSERT INTO lap_tags(activity_id, source, lap_index, tag, block, note)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(activity_id, source, lap_index)
        DO UPDATE SET
            tag=excluded.tag,
            block=excluded.block,
            note=excluded.note;
    """, rows)
    conn.commit()
    return len(rows)


def clear_tags_for_activity(conn: sqlite3.Connection, activity_id: int) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM lap_tags WHERE activity_id = ? AND source='STRAVA_LAP';", (activity_id,))
    conn.commit()


def list_laps_with_class_and_tags(conn: sqlite3.Connection, activity_id: int, limit: int = 60) -> None:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            l.lap_index,
            l.elapsed_time_s,
            ROUND(l.distance_m, 1),
            ROUND(l.average_speed_m_s, 2),
            c.class_label,
            t.tag,
            t.block
        FROM laps_strava l
        LEFT JOIN laps_strava_classified c
          ON c.activity_id = l.activity_id AND c.lap_index = l.lap_index
        LEFT JOIN lap_tags t
          ON t.activity_id = l.activity_id AND t.source='STRAVA_LAP' AND t.lap_index = l.lap_index
        WHERE l.activity_id = ?
        ORDER BY l.lap_index
        LIMIT ?;
    """, (activity_id, limit))
    rows = cur.fetchall()

    print("\nLaps (Strava) + class + tags:")
    print("lap | elapsed_s | dist_m | v_m_s | class | tag | block")
    for r in rows:
        print(r)
    print("")


def summary_by_tag(conn: sqlite3.Connection, activity_id: int) -> None:
    """
    Agrégation simple par tag :
    - nb laps
    - durée totale
    - distance totale
    - vitesse moyenne pondérée par le temps (approx)
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            t.tag,
            t.block,
            COUNT(*) AS n_laps,
            SUM(l.elapsed_time_s) AS total_s,
            SUM(l.distance_m) AS total_m,
            ROUND( (SUM(l.average_speed_m_s * l.elapsed_time_s) / NULLIF(SUM(l.elapsed_time_s),0)), 3) AS v_time_weighted
        FROM lap_tags t
        JOIN laps_strava l
          ON l.activity_id = t.activity_id AND l.lap_index = t.lap_index
        WHERE t.activity_id = ? AND t.source='STRAVA_LAP'
        GROUP BY t.tag, t.block
        ORDER BY
            CASE WHEN t.block IS NULL THEN 9
                 WHEN t.block='WARMUP' THEN 1
                 WHEN t.block='MAIN' THEN 2
                 WHEN t.block='COOLDOWN' THEN 3
                 ELSE 8 END,
            total_s DESC;
    """, (activity_id,))
    rows = cur.fetchall()

    print("\nRésumé par tag:")
    print("tag | block | n_laps | total_s | total_m | v_time_weighted")
    for r in rows:
        print(r)
    print("")


def list_recent_runs(conn: sqlite3.Connection, limit: int = 20) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT activity_id, start_date_local, name
        FROM activities
        WHERE sport_type IN ('Run','Trail Run') AND streams_status='OK'
        ORDER BY start_date_local DESC
        LIMIT ?;
    """, (limit,))
    return cur.fetchall()


def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    print("\nDernières activités RUN/TRAIL (streams OK):")
    for (aid, dt, name) in list_recent_runs(conn, 20):
        print(f"{aid} | {dt} | {name}")

    activity_id = int(input("\nactivity_id = ").strip())

    # Actions guidées: on propose un set de tags par défaut (tu pourras modifier)
    print("\nActions:")
    print("  1) Voir laps + class + tags")
    print("  2) Reset tags (supprime tous les tags de cette activité)")
    print("  3) Appliquer tags 'séance exemple' (pour 16696995539)")
    print("  4) Résumé par tag")
    choice = input("\nChoix = ").strip()

    if choice == "1":
        list_laps_with_class_and_tags(conn, activity_id, limit=80)

    elif choice == "2":
        clear_tags_for_activity(conn, activity_id)
        print("[OK] Tags supprimés.")
        list_laps_with_class_and_tags(conn, activity_id, limit=80)

    elif choice == "3":
        # Exemple basé sur ta séance:
        # - strides 3x15": laps 6,8,10 (efforts)
        # - recup strides: laps 7,9,11
        # - set 3x3': laps 13,15,17 (efforts)
        # - recup 105s: laps 14,16,18
        # - set 4x90": laps 20,22,24,26 ; recup 60": laps 21,23,25,27
        # - set 4x30": laps 29,31,33,35 ; recup 40": laps 30,32,34,36
        # - ignore lap 12 (skip)
        # - cooldown-ish: lap 38 (long)
        tag_strava_laps(conn, activity_id, [6, 8, 10], tag="strides_3x15s", block="WARMUP")
        tag_strava_laps(conn, activity_id, [7, 9, 11], tag="strides_recup_60s", block="WARMUP")
        tag_strava_laps(conn, activity_id, [13, 15, 17], tag="set_3x3min", block="MAIN")
        tag_strava_laps(conn, activity_id, [14, 16, 18], tag="recup_105s", block="MAIN")
        tag_strava_laps(conn, activity_id, [20, 22, 24, 26], tag="set_4x90s", block="MAIN")
        tag_strava_laps(conn, activity_id, [21, 23, 25, 27], tag="recup_60s", block="MAIN")
        tag_strava_laps(conn, activity_id, [29, 31, 33, 35], tag="set_4x30s", block="MAIN")
        tag_strava_laps(conn, activity_id, [30, 32, 34, 36], tag="recup_40s", block="MAIN")
        tag_strava_laps(conn, activity_id, [38], tag="cooldown", block="COOLDOWN", note="Retour au calme")
        print("[OK] Tags appliqués (exemple).")
        list_laps_with_class_and_tags(conn, activity_id, limit=80)
        summary_by_tag(conn, activity_id)

    elif choice == "4":
        summary_by_tag(conn, activity_id)

    else:
        print("Choix invalide.")

    conn.close()


if __name__ == "__main__":
    main()
