CREATE TABLE incidents (
    incident_number     TEXT PRIMARY KEY,
    report_number        TEXT,
    report_date           TIMESTAMPTZ,
    occurred_from_date    TIMESTAMPTZ,
    occurred_to_date      TIMESTAMPTZ,
    day_of_week            TEXT,
    part                    TEXT,          -- 'Part I' / 'Part II'
    crime_against           TEXT,          -- Person / Property / Society
    nibrs_ucr_code          TEXT,
    nibrs_offense           TEXT,
    nibrs_bucket            TEXT,
    victim_count            INTEGER,
    street_address          TEXT,
    location_type           TEXT,
    location                GEOGRAPHY(POINT, 4326),  -- PostGIS point, built from lat/long
    firearm_involved        BOOLEAN,
    family_violence_indicator BOOLEAN,
    bias_motivation_involved  BOOLEAN,
    gang_activity_involved    BOOLEAN,
    event_watch              TEXT,
    zone                     INTEGER,
    beat                     TEXT,
    district                 INTEGER,
    npu                      TEXT,
    neighborhood              TEXT,
    ingested_at               TIMESTAMPTZ DEFAULT now()
);