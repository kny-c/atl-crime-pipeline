"""
backfill.py

One-time script to pull ALL historical Atlanta crime incidents from the
public ArcGIS FeatureServer and load them into Postgres/PostGIS.

Run this once (or a few times while testing). After this, fetch_new.py
handles ongoing periodic updates.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv
import io
import csv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]  # from Neon dashboard, in .env
BASE_URL = (
    "https://services3.arcgis.com/Et5Qfajgiyosiw4d/arcgis/rest/services/"
    "OpenDataWebsite_Crime_view/FeatureServer/0/query"
)

PAGE_SIZE = 2000  # how many records to request per page
MAX_WORKERS = 8 # how many pages to fetch in parallel

# TEST MODE: set to a small number (e.g. 2) to only pull a couple pages
# and verify everything works before committing to the full 302K backfill.
# Set to None to run the full backfill.
MAX_PAGES = None


def epoch_ms_to_datetime(ms):
    """Convert ArcGIS's epoch-milliseconds date format to a real datetime."""
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        print(f" Warning: couldn't parse date value {ms!r}, storing as NULL")
        return None

def to_bool(val):
    """Convert Atlanta's yes/no/true/false strings into real booleans."""
    if val is None:
        return None
    return str(val).strip().lower() in ("yes", "true")

def get_total_count():
    """Ask the API how many total records match, to plan up front how many pages to fetch."""
    params = {
        "f": "json",
        "where": "1=1",
        "returnCountOnly": True,
    }
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()["count"]


def fetch_page(offset):
    """Request one page of records from the ArcGIS API."""
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "resultRecordCount": PAGE_SIZE,
        "resultOffset": offset,
    }
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def insert_batch(cursor, features):
    """Bulk-load a batch using Postgres COPY instead of RBR Inserts.
    
    Two steps:
    1. COPY the raw data into a plain staging table (fast, no POSTGIS conversion happening yet).
    2. One single INSERT...SELECT moves it from staging into the real incidents table, building the POSTGIS point and de-duping
    against exisitng rows, all in one set-based operation.
    """
    # Build an in-memory CSV (not a real file on disk, io.StringIO just acts like a file, but lives in memory).
    buffer = io.StringIO()
    writer = csv.writer(buffer)

    # Features is a list of dicts, each with an "attributes" dict inside. Write each row to the CSV.
    for feature in features:
        a = feature["attributes"]
        writer.writerow([
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
        ])

    buffer.seek(0) # rewind to the cursor to the beginning of file so COPY read from the beginning

    # Clear out any leftovers from a previous batch before loading this one.
    cursor.execute("TRUNCATE incidents_staging")

    # The actual bulk load -- this is the fast part. NULL '' tells Postgres
    # that an empty CSV field means NULL, not an empty string.
    cursor.copy_expert(
        "COPY incidents_staging FROM STDIN WITH (FORMAT CSV, NULL '')",
        buffer,
    )

    # Move from staging into the real table. This is where the PostGIS
    # point actually gets built, and where ON CONFLICT does the same
    # dedup job it did before -- just for a whole batch at once instead
    # of row by row.
    # ALSO!! The coordinates get transformed
    cursor.execute("""
        INSERT INTO incidents (
            incident_number, report_number, report_date,
            occurred_from_date, occurred_to_date, day_of_week,
            part, crime_against, nibrs_ucr_code, nibrs_offense,
            nibrs_bucket, victim_count, street_address, location_type,
            location,
            firearm_involved, family_violence_indicator,
            bias_motivation_involved, gang_activity_involved,
            event_watch, zone, beat, district, npu, neighborhood
        )
        SELECT
            incident_number, report_number, report_date,
            occurred_from_date, occurred_to_date, day_of_week,
            part, crime_against, nibrs_ucr_code, nibrs_offense,
            nibrs_bucket, victim_count, street_address, location_type,
            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography,
            firearm_involved, family_violence_indicator,
            bias_motivation_involved, gang_activity_involved,
            event_watch, zone, beat, district, npu, neighborhood
        FROM incidents_staging
        ON CONFLICT (incident_number) DO NOTHING
    """)

def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cursor = conn.cursor()

    total_count = get_total_count()
    print(f"Total records to fetch: {total_count}")

    # Build the full list of offsets up front, so we can fetch pages in parallel.
    offsets = list(range(0, total_count, PAGE_SIZE)) # [ 0, 2000, 4000, ... ]
    if MAX_PAGES is not None:
        offsets = offsets[:MAX_PAGES]
        print(f"MAX_PAGES={MAX_PAGES} (test mode) -- Only fetching "
              f"{len(offsets)} pages.")

    total_inserted = 0
    pages_done = 0

    # ThreadPoolExecutor fires off multiple fetch_page() calls at once,
    # instead of waiting for each one to finish
    # Still inserting into DB one batch at a time, to avoid overwhelming the DB with too many concurrent inserts.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all the fetch jobs. This returns immediately - the
        # requests happen in the background across worker threads.
        # Gives each worker a page offset
        # .submit() takes in a function and its arguments, and returns a Future object that represents the eventual result of that function call.
        future_to_offset = {}
        for offset in offsets:
            future = executor.submit(fetch_page, offset)
            future_to_offset[future] = offset # Add future to dict, with offset as the value. This allows us to know which offset corresponds to which future when it completes.

        # A FUTURE is a placeholder for a result that hasn't been computed yet. as_completed() yields futures as they complete, regardless of the order they were submitted in.

        # as_completed() hands each result as soon as it's ready,
        # not necessarily in the order we submitted them.
        for future in as_completed(future_to_offset):
            offset = future_to_offset[future]
            try:
                data = future.result() # This will block until the result is ready, or raise an exception if the fetch failed.
            except Exception as e:
                print(f"Error fetching or inserting page at offset {offset}: {e}")
                continue 

            features = data.get("features", [])
            if features:
                insert_batch(cursor, features)
                conn.commit()
                total_inserted += len(features)

            pages_done += 1
            print(f" Pages {pages_done}/{len(offsets)} done, "
                  f"(offset {offset}), Running total: {total_inserted}")

    cursor.close()
    conn.close()
    print(f"Backfill complete. Total records inserted: {total_inserted}")

if __name__ == "__main__":
    main()