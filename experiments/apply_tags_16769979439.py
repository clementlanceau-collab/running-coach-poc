import sqlite3
from typing import List, Tuple, Optional

DB_PATH = "running.db"


def ensure_lap_tags_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lap_tags (
        activity_id INTEGER NOT NULL,
        source TEXT NOT NULL,              -- 'STRAVA_LAP'
        lap_index INTEGER NOT NULL,
        tag TEXT,
        block TEXT,                        -- 'WARMUP' / 'MAIN' / 'COOLDOWN'
        PRIMARY KEY (activity_id, source, lap_index)
    );
    """)
    conn.commit()


def clear_tags(conn: sqlite3.Connection, activity_id: int) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM lap_tags WHERE activity_id = ? AND source='STRAVA_LAP';", (activity_id,))
    conn.commit()


def apply_tags(conn: sqlite3.Connection, activity_id: int, mappings: List[Tuple[int, str, str]]) -> None:
    """
    mappings: list of (lap_index, tag, block)
    """
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO lap_tags(activity_id, source, lap_index, tag, block)
        VALUES (?, 'STRAVA_LAP', ?, ?, ?)
        ON CONFLICT(activity_id, source, lap_index) DO UPDATE SET
            tag=excluded.tag,
            block=excluded.block;
    """, [(activity_id, lap_index, tag, block) for (lap_index, tag, block) in mappings])
    conn.commit()


def preview(conn: sqlite3.Connection, activity_id: int) -> None:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            l.lap_index, l.elapsed_time_s, ROUND(l.distance_m,1), ROUND(l.average_speed_m_s,2),
            ROUND(l.average_heartrate,1),
            t.block, t.tag
        FROM laps_strava l
        LEFT JOIN lap_tags t
          ON t.activity_id=l.activity_id AND t.source='STRAVA_LAP' AND t.lap_index=l.lap_index
        WHERE l.activity_id=?
        ORDER BY l.lap_index ASC;
    """, (activity_id,))
    rows = cur.fetchall()

    print("\nLaps + tags:")
    print("lap | elapsed_s | dist_m | v_m_s | avg_hr | block | tag")
    for r in rows:
        print(r)


def build_preset_16769979439() -> List[Tuple[int, str, str]]:
    """
    Tags décidés à partir de ton listing laps Strava.
    Objectif: lecture coach + comparaison robuste.
    """
    m: List[Tuple[int, str, str]] = []

    # WARMUP: 1-4
    for lap in [1, 2, 3, 4]:
        m.append((lap, "warmup", "WARMUP"))

    # Strides: 4x200m + récup (200m rapides: 5,7,9,11 ; récup: 6,8,10 ; lap12 transition)
    for lap in [5, 7, 9, 11]:
        m.append((lap, "strides_4x200m", "WARMUP"))
    for lap in [6, 8, 10]:
        m.append((lap, "strides_recup", "WARMUP"))
    m.append((12, "transition", "WARMUP"))

    # MAIN: 4x600 (13,19,25,31) + récup (14,20,26,32)
    for lap in [13, 19, 25, 31]:
        m.append((lap, "set_4x600m", "MAIN"))
    for lap in [14, 20, 26, 32]:
        m.append((lap, "recup_600m", "MAIN"))

    # MAIN: 4x400 (15,21,27,33) + récup (16,22,28,34)
    for lap in [15, 21, 27, 33]:
        m.append((lap, "set_4x400m", "MAIN"))
    for lap in [16, 22, 28, 34]:
        m.append((lap, "recup_400m", "MAIN"))

    # MAIN: 4x200 (17,23,29,35) + récup (18,24,30)
    for lap in [17, 23, 29, 35]:
        m.append((lap, "set_4x200m", "MAIN"))
    for lap in [18, 24, 30]:
        m.append((lap, "recup_200m", "MAIN"))

    # Lap 36: grosse coupure (530s / 274m) => OTHER (on le tagge pour le voir, mais il ne sera pas WORK/RECUP)
    m.append((36, "pause_stop", "COOLDOWN"))

    # COOLDOWN: 37-39
    for lap in [37, 38, 39]:
        m.append((lap, "cooldown", "COOLDOWN"))

    return m


def main():
    activity_id = 16769979439

    conn = sqlite3.connect(DB_PATH)
    ensure_lap_tags_table(conn)

    print(f"\nActivity = {activity_id}")
    print("Actions:")
    print("  1) Aperçu tags actuels")
    print("  2) Reset tags (supprime tags Strava laps)")
    print("  3) Appliquer preset tags (route 17/12)")
    print("  4) Reset + preset + aperçu")
    choice = input("\nChoix = ").strip()

    if choice == "1":
        preview(conn, activity_id)

    elif choice == "2":
        clear_tags(conn, activity_id)
        print("[OK] Tags supprimés.")
        preview(conn, activity_id)

    elif choice == "3":
        mappings = build_preset_16769979439()
        apply_tags(conn, activity_id, mappings)
        print("[OK] Preset tags appliqués.")
        preview(conn, activity_id)

    elif choice == "4":
        clear_tags(conn, activity_id)
        mappings = build_preset_16769979439()
        apply_tags(conn, activity_id, mappings)
        print("[OK] Reset + preset appliqués.")
        preview(conn, activity_id)

    else:
        print("Choix invalide.")

    conn.close()


if __name__ == "__main__":
    main()
