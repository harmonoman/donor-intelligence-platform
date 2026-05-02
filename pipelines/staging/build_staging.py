"""
Staging Build Script
Ticket 3.2 — Staging SQL + Load Script

Transforms raw FEC contributions into a clean staging table.

Pipeline flow:
    1. Load raw data from BigQuery (for given execution_date)
    2. Filter to IND entity type only
    3. Apply normalization (Python utility — single source of truth)
    4. Parse TRANSACTION_DT from MMDDYYYY to DATE
    5. MERGE into staging.stg_contributions (keyed on SUB_ID)

Why normalization is NOT in SQL:
    All normalization lives in pipelines/utils/normalize.py.
    Applying it in Python ensures staging and identity resolution
    use identical logic. SQL normalization would create a second
    implementation that could silently drift.

Why MERGE instead of overwrite:
    Staging accumulates records across multiple load dates.
    MERGE updates existing records and inserts new ones without
    creating duplicates. The raw layer uses partition overwrite
    (one day = one drawer). Staging uses MERGE (one record = one row).
"""

import argparse
import sys
from datetime import date, datetime

import pandas as pd
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from pipelines.utils.env import get_required_env, load_env
from pipelines.utils.normalize import normalize_address, normalize_name, normalize_zip

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STAGING_TABLE = "staging.stg_contributions"
MERGE_KEY = "SUB_ID"


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def load_raw_data(
    client: bigquery.Client,
    project_id: str,
    execution_date: date,
) -> pd.DataFrame:
    """
    Load raw FEC contributions for a specific execution date.
    Returns a DataFrame with original FEC column names.
    """
    query = f"""
        SELECT *
        FROM `{project_id}.raw.fec_contributions`
        WHERE _load_date = DATE('{execution_date.isoformat()}')
    """
    print(f"  Loading raw data for {execution_date}...")
    df = client.query(query).to_dataframe()

    # Convert _load_date from datetime.date to string immediately
    # Prevents PyArrow serialization errors downstream
    if "_load_date" in df.columns:
        df["_load_date"] = df["_load_date"].astype(str)

    print(f"  Raw rows loaded: {len(df):,}")
    return df


def filter_individuals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter to individual donors only (ENTITY_TP = 'IND').

    FEC files contain contributions from organizations, candidates,
    and committees in addition to individuals. This pipeline targets
    individual donors only.

    Documented in: docs/data-exploration.md — Staging Prerequisites
    """
    before = len(df)
    df = df[df["ENTITY_TP"] == "IND"].copy()
    after = len(df)
    print(f"  ENTITY_TP filter: {before:,} → {after:,} rows (removed {before - after:,} non-IND)")
    return df


def parse_contribution_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    parsed = pd.to_datetime(
        df["TRANSACTION_DT"],
        format="%m%d%Y",
        errors="coerce",
    )
    # Convert to string explicitly — None for unparseable dates
    df["contribution_date"] = [
        d.strftime("%Y-%m-%d") if pd.notna(d) else None
        for d in parsed
    ]

    null_dates = sum(1 for d in df["contribution_date"] if d is None)
    if null_dates > 0:
        print(f"  ⚠️  {null_dates:,} records with unparseable TRANSACTION_DT")

    return df


def apply_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply shared normalization utility to name, address, and ZIP fields.

    Uses pipelines/utils/normalize.py — the single source of truth.
    This exact same utility will be used in identity resolution.
    Consistency between layers is guaranteed by using one function.
    """
    df = df.copy()

    df["donor_name_normalized"] = df["NAME"].apply(normalize_name)
    df["donor_address_normalized"] = (
        df["CITY"].fillna("") + " " + df["STATE"].fillna("")
    ).apply(normalize_address)
    df["zip_normalized"] = df["ZIP_CODE"].apply(normalize_zip)

    return df


def prepare_staging_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and rename fields for the staging table.
    Preserves raw fields alongside normalized fields for auditability.
    """
    return pd.DataFrame({
        "sub_id":                   df["SUB_ID"],
        "cmte_id":                  df["CMTE_ID"],
        "tran_id":                  df["TRAN_ID"],
        "name_raw":                 df["NAME"],
        "city_raw":                 df["CITY"],
        "state":                    df["STATE"],
        "zip_raw":                  df["ZIP_CODE"],
        "donor_name_normalized":    df["donor_name_normalized"],
        "donor_address_normalized": df["donor_address_normalized"],
        "zip_normalized":           df["zip_normalized"],
        "contribution_amount": df["TRANSACTION_AMT"].astype(str),  # string → cast to NUMERIC in MERGE
        "contribution_date":        df["contribution_date"],
        "entity_type":              df["ENTITY_TP"],
        "_load_date":               df["_load_date"].astype(str),  # convert date → string
    })


def ensure_staging_table_exists(
    client: bigquery.Client,
    project_id: str,
) -> None:
    """
    Create staging table if it does not exist.
    Safe to call multiple times — no-op if table already exists.
    """
    full_table_id = f"{project_id}.{STAGING_TABLE}"

    schema = [
        bigquery.SchemaField("sub_id",                   "STRING",  "NULLABLE"),
        bigquery.SchemaField("cmte_id",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("tran_id",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("name_raw",                 "STRING",  "NULLABLE"),
        bigquery.SchemaField("city_raw",                 "STRING",  "NULLABLE"),
        bigquery.SchemaField("state",                    "STRING",  "NULLABLE"),
        bigquery.SchemaField("zip_raw",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("donor_name_normalized",    "STRING",  "NULLABLE"),
        bigquery.SchemaField("donor_address_normalized", "STRING",  "NULLABLE"),
        bigquery.SchemaField("zip_normalized",           "STRING",  "NULLABLE"),
        bigquery.SchemaField("contribution_amount",      "NUMERIC", "NULLABLE"),
        bigquery.SchemaField("contribution_date",        "DATE",    "NULLABLE"),
        bigquery.SchemaField("entity_type",              "STRING",  "NULLABLE"),
        bigquery.SchemaField("_load_date",               "DATE",    "NULLABLE"),
    ]

    table = bigquery.Table(full_table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="_load_date",
    )

    try:
        client.create_table(table)
        print(f"  Created table: {full_table_id}")
    except Conflict:
        pass  # table already exists — safe to continue


def merge_into_staging(
    client: bigquery.Client,
    project_id: str,
    df: pd.DataFrame,
) -> int:
    """
    MERGE normalized records into staging table.

    MERGE key: SUB_ID (100% populated — confirmed in data exploration)

    Why MERGE and not overwrite:
        Raw layer uses partition overwrite — each date is isolated.
        Staging accumulates across dates — MERGE prevents duplicates
        while allowing reruns to update existing records safely.
    """
    # MERGE key: SUB_ID only
    # Fallback key (donor_name_normalized + contribution_date + contribution_amount)
    # was retired — SUB_ID confirmed 100% populated across 1.98M rows
    # See: docs/data-exploration.md — MERGE Key Decision
    if df.empty:
        print("  No records to merge.")
        return 0

    # Write to a temporary table first
    temp_table_id = f"{project_id}.staging._stg_contributions_temp"

    # Explicit schema prevents autodetect type mismatches
    # contribution_amount must be NUMERIC (not FLOAT64)
    # contribution_date must be DATE (not DATETIME or TIMESTAMP)
    temp_schema = [
        bigquery.SchemaField("sub_id",                   "STRING",  "NULLABLE"),
        bigquery.SchemaField("cmte_id",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("tran_id",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("name_raw",                 "STRING",  "NULLABLE"),
        bigquery.SchemaField("city_raw",                 "STRING",  "NULLABLE"),
        bigquery.SchemaField("state",                    "STRING",  "NULLABLE"),
        bigquery.SchemaField("zip_raw",                  "STRING",  "NULLABLE"),
        bigquery.SchemaField("donor_name_normalized",    "STRING",  "NULLABLE"),
        bigquery.SchemaField("donor_address_normalized", "STRING",  "NULLABLE"),
        bigquery.SchemaField("zip_normalized",           "STRING",  "NULLABLE"),
        bigquery.SchemaField("contribution_amount", "STRING", "NULLABLE"),  # cast to NUMERIC in MERGE
        bigquery.SchemaField("contribution_date",        "STRING",  "NULLABLE"),  # string → cast in MERGE
        bigquery.SchemaField("entity_type",              "STRING",  "NULLABLE"),
        bigquery.SchemaField("_load_date",               "STRING",  "NULLABLE"),  # string → cast in MERGE
    ]

    temp_job_config = bigquery.LoadJobConfig(
        schema=temp_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    client.load_table_from_dataframe(
        df, temp_table_id, job_config=temp_job_config
    ).result(timeout=300)

    # MERGE from temp into staging
    merge_sql = f"""
        MERGE `{project_id}.{STAGING_TABLE}` AS target
        USING `{temp_table_id}` AS source
        ON target.sub_id = source.sub_id

        WHEN MATCHED THEN UPDATE SET
            cmte_id                  = source.cmte_id,
            tran_id                  = source.tran_id,
            name_raw                 = source.name_raw,
            city_raw                 = source.city_raw,
            state                    = source.state,
            zip_raw                  = source.zip_raw,
            donor_name_normalized    = source.donor_name_normalized,
            donor_address_normalized = source.donor_address_normalized,
            zip_normalized           = source.zip_normalized,
            contribution_amount      = SAFE_CAST(source.contribution_amount AS NUMERIC),
            contribution_date        = SAFE.PARSE_DATE('%Y-%m-%d', source.contribution_date),
            entity_type              = source.entity_type,
            _load_date               = SAFE.PARSE_DATE('%Y-%m-%d', source._load_date)

        WHEN NOT MATCHED THEN INSERT (
            sub_id, cmte_id, tran_id, name_raw, city_raw, state,
            zip_raw, donor_name_normalized, donor_address_normalized,
            zip_normalized, contribution_amount, contribution_date,
            entity_type, _load_date
        ) VALUES (
            source.sub_id, source.cmte_id, source.tran_id,
            source.name_raw, source.city_raw, source.state,
            source.zip_raw, source.donor_name_normalized,
            source.donor_address_normalized, source.zip_normalized,
            SAFE_CAST(source.contribution_amount AS NUMERIC),
            SAFE.PARSE_DATE('%Y-%m-%d', source.contribution_date),
            source.entity_type,
            SAFE.PARSE_DATE('%Y-%m-%d', source._load_date)
        )
    """

    client.query(merge_sql).result(timeout=300)

    # Clean up temp table
    client.delete_table(temp_table_id, not_found_ok=True)

    print(f"  Merged {len(df):,} records into {STAGING_TABLE}")
    return len(df)


def count_staging_rows(
    client: bigquery.Client,
    project_id: str,
    execution_date: date,
) -> int:
    """Count staging rows for a specific load date."""
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{project_id}.{STAGING_TABLE}`
        WHERE _load_date = DATE('{execution_date.isoformat()}')
    """
    result = list(client.query(query).result())
    return result[0]["row_count"]


def run_staging(
    client: bigquery.Client,
    project_id: str,
    execution_date: date,
) -> int:
    """
    Run full staging transformation for a given execution date.
    Returns number of records merged.
    """
    ensure_staging_table_exists(client, project_id)

    df = load_raw_data(client, project_id, execution_date)
    df = filter_individuals(df)
    df = parse_contribution_date(df)
    df = apply_normalization(df)
    df = prepare_staging_dataframe(df)

    return merge_into_staging(client, project_id, df)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Build staging.stg_contributions from raw FEC data."
    )
    parser.add_argument(
        "--execution-date",
        required=True,
        help="Execution date (YYYY-MM-DD) — must match a raw partition date",
    )
    return parser.parse_args()


def main():
    load_env()
    args = parse_args()

    try:
        execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d").date()
    except ValueError:
        print(f"ERROR: Invalid date format '{args.execution_date}'. Use YYYY-MM-DD.")
        sys.exit(1)

    project_id = get_required_env("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    print(f"\n{'='*60}")
    print("STAGING BUILD")
    print(f"{'='*60}")
    print(f"  Execution date : {execution_date}")
    print(f"  Target table   : {project_id}.{STAGING_TABLE}")
    print(f"{'='*60}\n")

    rows_merged = run_staging(client, project_id, execution_date)

    staging_count = count_staging_rows(client, project_id, execution_date)

    print(f"\n{'='*60}")
    print("STAGING COMPLETE")
    print(f"{'='*60}")
    print(f"  Records merged : {rows_merged:,}")
    print(f"  Staging count  : {staging_count:,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
