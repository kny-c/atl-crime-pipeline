"""
fetch_new.py

Recurring script — meant to be run periodically (every few hours) via a
scheduler. Pulls only incidents newer than what's already in the database,
instead of re-pulling everything like backfill.py does.

Deliberately simpler than backfill.py: at this scale (a handful to a few
hundred new records per run, not 300K+), plain sequential fetch + insert
is plenty fast. No concurrency, no COPY/staging needed here.
"""

import os
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

BASE_URL = (
    "https://services3.arcgis.com/Et5Qfajgiyosiw4d/arcgis/rest/services/"
    "OpenDataWebsite_Crime_view/FeatureServer/0/query"
)


def epoch_ms_to_datetime(ms):
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        print(f"  Warning: couldn't parse date value {ms!r}, storing as NULL")
        return None


def to_bool(val):
    if val is None:
        return None
    return str(val).strip().lower() in ("yes", "true")


def get_last_run_time(cursor):
    """Find when this script last ran successfully -- based on OUR OWN
    log, not Atlanta's data, so a bad date anywhere in incidents can
    never break this again."""
    cursor.execute("SELECT MAX(run_at) FROM ingestion_log")
    return cursor.fetchone()[0]


def save_run(cursor, records_found, records_new):
    """Record that this run happened, for next time to read."""
    cursor.execute(
        "INSERT INTO ingestion_log (records_found, records_new) VALUES (%s, %s)",
        (records_found, records_new),
    )


def fetch_new_records(since_date):
    """Ask the API for records reported after `since_date`, paginating
    in case there are more than fit in a single response (e.g. if this
    script hasn't been run in a while and a lot has piled up)."""
    since_str = since_date.strftime("%Y-%m-%d %H:%M:%S")
    where_clause = f"ReportDate > timestamp '{since_str}'"

    all_features = []
    offset = 0
    page_size = 2000

    while True:
        params = {
            "f": "json",
            "where": where_clause,
            "outFields": "*",
            "orderByFields": "ReportDate ASC",
            "resultRecordCount": page_size,
            "resultOffset": offset,
        }
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        all_features.extend(features)

        # If the server tells us there's more beyond what we got, or we
        # got a full page (meaning there might be more), keep going.
        if not data.get("exceededTransferLimit") and len(features) < page_size:
            break

        offset += page_size

    return all_features


def insert_batch(cursor, features):
    """Same insert logic as backfill.py -- plain executemany is fine here
    since batches are small."""
    rows = []
    for feature in features:
        a = feature["attributes"]
        rows.append(
            (
                a.get("IncidentNumber"),
                a.get("ReportNumber"),
                epoch_ms_to_datetime(a.get("ReportDate")),
                epoch_ms_to_datetime(a.get("OccurredFromDate")),
                epoch_ms_to_datetime(a.get("OccurredToDate")),
                a.get("Day_of_the_week"),
                a.get("Part"),
                a.get("Crime_Against"),
                a.get("NibrsUcrCode"),
                a.get("NIBRS_Offense"),
                a.get("NIBRS_Bucket"),
                a.get("Vic_Count"),
                a.get("StreetAddress"),
                a.get("LocationType"),
                a.get("Longitude"),
                a.get("Latitude"),
                to_bool(a.get("FireArmInvolved")),
                to_bool(a.get("GAFamilyViolenceIndicator")),
                to_bool(a.get("IsBiasMotivationInvolved")),
                to_bool(a.get("CriminalGangActivityInvolved")),
                a.get("event_watch"),
                a.get("Zone_int"),
                a.get("BEAT_Text"),
                a.get("DISTRICT"),
                a.get("NPU"),
                a.get("NhoodName"),
            )
        )

    cursor.executemany(
        """
        INSERT INTO incidents (
            incident_number, report_number, report_date,
            occurred_from_date, occurred_to_date, day_of_week,
            part, crime_against, nibrs_ucr_code, nibrs_offense,
            nibrs_bucket, victim_count, street_address, location_type,
            location,
            firearm_involved, family_violence_indicator,
            bias_motivation_involved, gang_activity_involved,
            event_watch, zone, beat, district, npu, neighborhood
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (incident_number) DO NOTHING
        """,
        rows,
    )


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cursor = conn.cursor()

    since_date = get_last_run_time(cursor)
    if since_date is None:
        print("No previous run found -- checking the last 24 hours.")

        since_date = datetime.now(timezone.utc) - timedelta(hours=24)

    print(f"Checking for records reported after {since_date}...")
    features = fetch_new_records(since_date)

    actually_new = 0
    if not features:
        print("No new records. Database is up to date.")
    else:
        cursor.execute("SELECT COUNT(*) FROM incidents")
        count_before = cursor.fetchone()[0]

        insert_batch(cursor, features)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM incidents")
        count_after = cursor.fetchone()[0]
        actually_new = count_after - count_before

        print(f"API returned {len(features)} record(s).")
        print(f"Actually inserted {actually_new} new record(s).")

    save_run(cursor, len(features), actually_new)
    conn.commit()

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()