"""
Identity Resolution Build Script
Ticket 4.2 — dim_donors Core Matching Logic

Assigns stable donor identities to normalized staging records.

Matching rules (applied in strict order):
    Rule 1: donor_name_normalized + zip_normalized (exact match)
    Rule 2: donor_name_normalized + donor_address_normalized (exact match)
    No match: new donor_id created

Why hash-based donor_id:
    Same input always produces same donor_id across runs.
    Deterministic and reproducible — no random UUIDs.
    donor_id = MD5(canonical_matching_key)

Why Rule 1 before Rule 2:
    ZIP is more specific than city+state.
    Two donors in the same ZIP with the same name are more likely
    the same person than two donors in the same city+state.
    Rule 1 is the stronger signal.

Key architectural decision:
    Same canonical key = same donor. Always.
    In a deterministic batch hash system, two records sharing a
    canonical key cannot be distinguished as "same person, two
    contributions" vs "two different people with identical fields."
    Both resolve to the same donor_id. identity_conflict is always FALSE.
    True collision detection requires incremental matching against a
    known-good donor registry — deferred to post-MVP.
"""

import argparse
import csv
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from pipelines.utils.env import get_required_env, load_env

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DIM_DONORS_TABLE = "core.dim_donors"
UNRESOLVED_TABLE = "core.dim_donors_unresolved"
STAGING_TABLE = "staging.stg_contributions"


# ---------------------------------------------------------------------------
# Table management
# ---------------------------------------------------------------------------

def ensure_dim_donors_exists(
    client: bigquery.Client,
    project_id: str,
    table_id: str = DIM_DONORS_TABLE,
) -> None:
    """Create dim_donors table if it does not exist."""
    full_table_id = f"{project_id}.{table_id}"
    schema = [
        bigquery.SchemaField("donor_id",                 "STRING", "REQUIRED"),
        bigquery.SchemaField("sub_id",                   "STRING", "NULLABLE"),
        bigquery.SchemaField("donor_name_normalized",    "STRING", "NULLABLE"),
        bigquery.SchemaField("donor_address_normalized", "STRING", "NULLABLE"),
        bigquery.SchemaField("zip_normalized",           "STRING", "NULLABLE"),
        bigquery.SchemaField("match_rule",               "STRING", "NULLABLE"),
        bigquery.SchemaField("identity_conflict",        "BOOL",   "NULLABLE"),
        bigquery.SchemaField("_load_date",               "DATE",   "NULLABLE"),
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
        pass


def ensure_unresolved_exists(
    client: bigquery.Client,
    project_id: str,
    table_id: str = UNRESOLVED_TABLE,
) -> None:
    """Create dim_donors_unresolved table if it does not exist."""
    full_table_id = f"{project_id}.{table_id}"
    schema = [
        bigquery.SchemaField("donor_id",                 "STRING", "NULLABLE"),
        bigquery.SchemaField("sub_id",                   "STRING", "NULLABLE"),
        bigquery.SchemaField("donor_name_normalized",    "STRING", "NULLABLE"),
        bigquery.SchemaField("donor_address_normalized", "STRING", "NULLABLE"),
        bigquery.SchemaField("zip_normalized",           "STRING", "NULLABLE"),
        bigquery.SchemaField("conflict_reason",          "STRING", "NULLABLE"),
        bigquery.SchemaField("_load_date",               "DATE",   "NULLABLE"),
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
        pass


# ---------------------------------------------------------------------------
# Fixture loading (for test validation)
# ---------------------------------------------------------------------------

def load_fixture_to_staging_temp(
    client: bigquery.Client,
    project_id: str,
    fixture_path: Path,
    execution_date: date,
    temp_table: str,
) -> None:
    """
    Load identity fixture CSV into a temporary staging table.
    Used for fixture validation before running against real data.
    """
    rows = []
    with open(fixture_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "sub_id":                   row["record_id"],
                "donor_name_normalized":    row["donor_name_normalized"],
                "donor_address_normalized": row["donor_address_normalized"],
                "zip_normalized":           row["zip_normalized"],
                "_load_date":               execution_date.isoformat(),
            })

    df = pd.DataFrame(rows)

    full_table_id = f"{project_id}.{temp_table}"
    schema = [
        bigquery.SchemaField("sub_id",                   "STRING", "NULLABLE"),
        bigquery.SchemaField("donor_name_normalized",    "STRING", "NULLABLE"),
        bigquery.SchemaField("donor_address_normalized", "STRING", "NULLABLE"),
        bigquery.SchemaField("zip_normalized",           "STRING", "NULLABLE"),
        bigquery.SchemaField("_load_date",               "STRING", "NULLABLE"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(df, full_table_id, job_config=job_config).result(timeout=300)
    print(f"  Loaded {len(df)} fixture records → {full_table_id}")


# ---------------------------------------------------------------------------
# Core identity resolution SQL
# ---------------------------------------------------------------------------

def build_identity_sql(
    project_id: str,
    source_table: str,
    execution_date: date,
) -> str:
    """
    Generate the identity resolution SQL.

    Two-pass matching logic:
    Pass 1 — Rule 1: match on donor_name_normalized + zip_normalized
    Pass 2 — Rule 2: for records with no ZIP, look up whether a Rule 1
              record shares the same name + address. If yes → inherit
              that canonical key (same donor_id). If no → new donor_id.

    Key principle:
    Same canonical key = same donor. Always.
    In a deterministic batch hash system, two records sharing a
    canonical key cannot be distinguished as "same person, two
    contributions" vs "two different people with identical fields."
    Both resolve to the same donor_id. This is a known limitation
    of deterministic matching on available FEC fields.

    donor_id = MD5(canonical_key) — deterministic and reproducible.
    identity_conflict is always FALSE in this implementation.
    """
    return f"""
    -- Step 1: Load source records
    WITH source AS (
        SELECT
            sub_id,
            donor_name_normalized,
            donor_address_normalized,
            zip_normalized,
            CAST(_load_date AS STRING) AS _load_date
        FROM `{project_id}.{source_table}`
        WHERE CAST(_load_date AS STRING) = '{execution_date.isoformat()}'
    ),

    -- Pass 1: Rule 1 — match on name + ZIP
    rule1_matches AS (
        SELECT
            sub_id,
            donor_name_normalized,
            donor_address_normalized,
            zip_normalized,
            _load_date,
            CONCAT(donor_name_normalized, '|zip:', zip_normalized) AS canonical_key,
            'rule1' AS match_rule
        FROM source
        WHERE zip_normalized IS NOT NULL AND zip_normalized != ''
    ),

    -- Pass 2: Rule 2 candidates — records with no ZIP
    rule2_candidates AS (
        SELECT
            sub_id,
            donor_name_normalized,
            donor_address_normalized,
            zip_normalized,
            _load_date
        FROM source
        WHERE zip_normalized IS NULL OR zip_normalized = ''
    ),

    -- Pass 2: Rule 2 — look up whether a Rule 1 record shares name + address
    rule2_matches AS (
        SELECT
            r2.sub_id,
            r2.donor_name_normalized,
            r2.donor_address_normalized,
            r2.zip_normalized,
            r2._load_date,
            COALESCE(
                MIN(r1.canonical_key),
                CASE
                    WHEN r2.donor_address_normalized IS NOT NULL
                         AND r2.donor_address_normalized != ''
                        THEN CONCAT(r2.donor_name_normalized, '|addr:', r2.donor_address_normalized)
                    ELSE CONCAT(r2.donor_name_normalized, '|id:', r2.sub_id)
                END
            ) AS canonical_key,
            CASE
                WHEN MIN(r1.canonical_key) IS NOT NULL THEN 'rule2'
                WHEN r2.donor_address_normalized IS NOT NULL
                     AND r2.donor_address_normalized != '' THEN 'rule2'
                ELSE 'no_match'
            END AS match_rule
        FROM rule2_candidates r2
        LEFT JOIN rule1_matches r1
            ON r2.donor_name_normalized = r1.donor_name_normalized
            AND r2.donor_address_normalized = r1.donor_address_normalized
            AND r1.donor_address_normalized IS NOT NULL
            AND r1.donor_address_normalized != ''
        GROUP BY
            r2.sub_id, r2.donor_name_normalized, r2.donor_address_normalized,
            r2.zip_normalized, r2._load_date
    ),

    -- Combine both passes
    keyed AS (
        SELECT * FROM rule1_matches
        UNION ALL
        SELECT * FROM rule2_matches
    ),

    -- Step 3: Generate deterministic donor_id from canonical_key
    with_donor_id AS (
        SELECT
            *,
            TO_HEX(MD5(canonical_key)) AS donor_id
        FROM keyed
    ),

    -- Step 4: All records sharing a canonical key resolve to the same donor_id
    -- identity_conflict is always FALSE — same key always means same donor
    final AS (
        SELECT
            w.donor_id,
            w.sub_id,
            w.donor_name_normalized,
            w.donor_address_normalized,
            w.zip_normalized,
            w.match_rule,
            CAST(w._load_date AS STRING) AS _load_date,
            FALSE AS identity_conflict
        FROM with_donor_id w
    )

    SELECT * FROM final
    """


def run_identity_resolution(
    client: bigquery.Client,
    project_id: str,
    source_table: str = STAGING_TABLE,
    dim_donors_table: str = DIM_DONORS_TABLE,
    unresolved_table: str = UNRESOLVED_TABLE, # reserved — not populated until post-MVP
    execution_date: date = None,
) -> int:
    """
    Run full identity resolution for a given execution date.
    Returns number of records written to dim_donors.
    """
    ensure_dim_donors_exists(client, project_id, dim_donors_table)
    ensure_unresolved_exists(client, project_id, unresolved_table)  # reserved — not populated until post-MVP

    identity_sql = build_identity_sql(
        project_id, source_table, execution_date
    )

    # Write results to temp table
    temp_table = f"{project_id}.core._dim_donors_temp"
    temp_job = bigquery.QueryJobConfig(
        destination=temp_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    print(f"  Running identity resolution for {execution_date}...")
    client.query(identity_sql, job_config=temp_job).result(timeout=300)

    # MERGE into dim_donors
    merge_sql = f"""
        MERGE `{project_id}.{dim_donors_table}` AS target
        USING `{temp_table}` AS source
        ON target.sub_id = source.sub_id

        WHEN MATCHED THEN UPDATE SET
            donor_id                 = source.donor_id,
            donor_name_normalized    = source.donor_name_normalized,
            donor_address_normalized = source.donor_address_normalized,
            zip_normalized           = source.zip_normalized,
            match_rule               = source.match_rule,
            identity_conflict        = source.identity_conflict,
            _load_date               = SAFE.PARSE_DATE('%Y-%m-%d', source._load_date)

        WHEN NOT MATCHED THEN INSERT (
            donor_id, sub_id, donor_name_normalized,
            donor_address_normalized, zip_normalized,
            match_rule, identity_conflict, _load_date
        ) VALUES (
            source.donor_id, source.sub_id, source.donor_name_normalized,
            source.donor_address_normalized, source.zip_normalized,
            source.match_rule, source.identity_conflict,
            SAFE.PARSE_DATE('%Y-%m-%d', source._load_date)
        )
    """
    client.query(merge_sql).result(timeout=300)

    # Clean up temp table
    client.delete_table(temp_table, not_found_ok=True)

    # Count results
    count_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.{dim_donors_table}`
        WHERE _load_date = DATE('{execution_date.isoformat()}')
    """
    count = list(client.query(count_query).result())[0]["cnt"]
    print(f"  Identity resolution complete: {count:,} records in dim_donors")

    # Log match_rule distribution
    stats_query = f"""
        SELECT match_rule, COUNT(*) as cnt
        FROM `{project_id}.{dim_donors_table}`
        WHERE _load_date = DATE('{execution_date.isoformat()}')
        GROUP BY match_rule
        ORDER BY cnt DESC
    """
    stats = list(client.query(stats_query).result())
    print("  Match rule distribution:")
    for row in stats:
        print(f"    {row['match_rule']}: {row['cnt']:,}")

    return count


def count_dim_donors_rows(
    client: bigquery.Client,
    project_id: str,
    execution_date: date,
    table_id: str = DIM_DONORS_TABLE,
) -> int:
    """Count dim_donors rows for a specific load date."""
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{project_id}.{table_id}`
        WHERE _load_date = DATE('{execution_date.isoformat()}')
    """
    return list(client.query(query).result())[0]["row_count"]


def get_donor_id_for_record(
    client: bigquery.Client,
    project_id: str,
    sub_id: str,
    execution_date: date,
    table_id: str = DIM_DONORS_TABLE,
) -> str:
    """Get donor_id for a specific sub_id."""
    query = f"""
        SELECT donor_id
        FROM `{project_id}.{table_id}`
        WHERE sub_id = '{sub_id}'
        AND _load_date = DATE('{execution_date.isoformat()}')
        LIMIT 1
    """
    rows = list(client.query(query).result())
    return rows[0]["donor_id"] if rows else None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Build core.dim_donors from staging data."
    )
    parser.add_argument(
        "--execution-date",
        required=True,
        help="Execution date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--fixture-only",
        action="store_true",
        help="Run against fixture data only (for validation)",
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
    print("IDENTITY RESOLUTION")
    print(f"{'='*60}")
    print(f"  Execution date : {execution_date}")
    print(f"  Mode           : {'fixture validation' if args.fixture_only else 'full dataset'}")
    print(f"{'='*60}\n")

    if args.fixture_only:
        load_fixture_to_staging_temp(
            client, project_id,
            Path("tests/fixtures/identity_fixtures.csv"),
            execution_date,
            "staging.stg_identity_test",
        )
        count = run_identity_resolution(
            client, project_id,
            source_table="staging.stg_identity_test",
            dim_donors_table="core.dim_donors_test",
            unresolved_table="core.dim_donors_unresolved_test",
            execution_date=execution_date,
        )
    else:
        count = run_identity_resolution(
            client, project_id,
            execution_date=execution_date,
        )

    print(f"\n{'='*60}")
    print("IDENTITY RESOLUTION COMPLETE")
    print(f"{'='*60}")
    print(f"  Records in dim_donors : {count:,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
