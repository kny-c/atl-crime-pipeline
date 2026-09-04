"""
anomaly_baseline.py

Computes weekly incident counts per (zone, offense_type), then flags
weeks where the count deviates significantly from a trailing rolling
baseline (mean + N standard deviations).

Adjust ZONE_COL / OFFENSE_COL below to match your actual column names
in the `incidents` table if they differ.
"""

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()  

# --- Config ---
DATABASE_URL = os.environ["DATABASE_URL"]  # same env var pattern as backfill.py
ZONE_COL = "zone"              # INTEGER column, e.g. 1-6
OFFENSE_COL = "nibrs_bucket"    # consolidated offense category (29 categories, less sparse than nibrs_offense)
DATE_COL = "report_date"

ROLLING_WINDOW_WEEKS = 8   # how many past weeks count as "recent normal"
MIN_PERIODS = 4            # don't flag anomalies until we have at least this many weeks of baseline
STD_THRESHOLD = 2.5        # how many std devs above the mean counts as anomalous


def fetch_weekly_counts(engine) -> pd.DataFrame:
    """
    Aggregate raw incident rows into weekly counts per zone + offense type.
    date_trunc('week', ...) buckets each report_date into the Monday of its week.
    """
    query = text(f"""
        SELECT
            date_trunc('week', {DATE_COL}) AS week_start,
            {ZONE_COL} AS zone,
            {OFFENSE_COL} AS offense_type,
            COUNT(*) AS incident_count
        FROM incidents
        WHERE {DATE_COL} >= '2021-01-01'   -- filters out the known bad historical dates
          AND {ZONE_COL} IS NOT NULL       -- ~1,306 rows have no zone; drop rather than group as phantom category
          AND {OFFENSE_COL} IS NOT NULL    -- ~37,254 rows have no offense bucket; same reasoning
        GROUP BY 1, 2, 3
        ORDER BY zone, offense_type, week_start;
    """)
    return pd.read_sql(query, engine)


def compute_rolling_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (zone, offense_type) group independently, compute a trailing
    rolling mean/std of incident_count, then flag anomalies.
    """
    df = df.sort_values(["zone", "offense_type", "week_start"]).copy()

    grouped = df.groupby(["zone", "offense_type"])["incident_count"]

    # shift(1) excludes the CURRENT week from its own baseline -- otherwise
    # a spike would inflate the very average it's being compared against
    df["rolling_mean"] = grouped.transform(
        lambda s: s.shift(1).rolling(window=ROLLING_WINDOW_WEEKS, min_periods=MIN_PERIODS).mean()
    )
    df["rolling_std"] = grouped.transform(
        lambda s: s.shift(1).rolling(window=ROLLING_WINDOW_WEEKS, min_periods=MIN_PERIODS).std()
    )

    df["anomaly_threshold"] = df["rolling_mean"] + STD_THRESHOLD * df["rolling_std"]
    df["is_anomaly"] = df["incident_count"] > df["anomaly_threshold"]

    return df


def main():
    engine = create_engine(DATABASE_URL)

    print("Fetching weekly counts from Postgres...")
    weekly = fetch_weekly_counts(engine)
    print(f"  -> {len(weekly)} (zone, offense_type, week) rows")

    print("Computing rolling baselines...")
    result = compute_rolling_baseline(weekly)

    anomalies = result[result["is_anomaly"] == True].sort_values("week_start", ascending=False)
    print(f"\nFound {len(anomalies)} anomalous weeks out of {len(result)} total.")
    print(anomalies[["week_start", "zone", "offense_type", "incident_count", "rolling_mean", "rolling_std"]].head(20))

    result.to_csv("weekly_anomaly_results.csv", index=False)
    print("\nFull results written to weekly_anomaly_results.csv")


if __name__ == "__main__":
    main()