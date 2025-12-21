import csv
from datetime import date, timedelta

from app.adapters.strava import fetch_sessions


def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def main():
    sessions = fetch_sessions(per_page=60)

    with open("sessions.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date",
            "external_id",
            "source",
            "sport_type",
            "session_type",
            "duration_min",
            "distance_km",
            "elevation_m",
            "has_streams",
            "intensity_level",
            "device_name",
            "has_heartrate",
            "hr_source",
            "data_confidence_gps",
            "data_flags",
        ])

        for s in sorted(sessions, key=lambda x: x.date, reverse=True):
            if s.data_flags and len(s.data_flags) > 0:
                flags_str = "|".join(s.data_flags)
            else:
                flags_str = "OK"

            writer.writerow([
                s.date.isoformat(),
                s.external_id,
                s.source,
                s.sport_type,
                s.session_type,
                s.duration_min,
                round(s.distance_km, 3),
                s.elevation_m,
                s.has_streams,
                s.intensity_level,
                s.device_name or "UNKNOWN",
                s.has_heartrate if s.has_heartrate is not None else "UNKNOWN",
                s.hr_source or "UNKNOWN",
                s.data_confidence_gps if s.data_confidence_gps is not None else "UNKNOWN",
                flags_str,
            ])

    print("CSV généré : sessions.csv")

    weekly = {}
    for s in sessions:
        ws = week_start(s.date)
        if ws not in weekly:
            weekly[ws] = {"count": 0, "km": 0.0, "min": 0}
        weekly[ws]["count"] += 1
        weekly[ws]["km"] += s.distance_km
        weekly[ws]["min"] += s.duration_min

    print("Résumé hebdo (4 dernières semaines):")
    for ws in sorted(weekly.keys(), reverse=True)[:4]:
        we = ws + timedelta(days=6)
        print(
            f"- {ws} -> {we} | séances: {weekly[ws]['count']} | "
            f"km: {round(weekly[ws]['km'], 2)} | min: {weekly[ws]['min']}"
        )


if __name__ == "__main__":
    main()
