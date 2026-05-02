"""
Raw FEC Ingestion Script
Ticket 2.2 — Raw Ingestion Script

Loads FEC contribution CSV into BigQuery raw layer.

Usage:
    uv run python pipelines/ingest/load_raw_fec.py --execution-date 2025-01-01

What this does:
    1. Reads FEC sample CSV
    2. Adds _load_date column
    3. Loads to BigQuery with partition overwrite

Idempotency guarantee:
    Each execution_date is a separate BigQuery partition.
    WRITE_TRUNCATE overwrites that partition only.
    Rerunning the same date = same result, no duplicates.

    Think of it like a dated drawer in a filing cabinet:
    When you rerun, you empty that drawer and refill it.
    You never stack a second copy on top.
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from pipelines.ingest.schema import RAW_FEC_SCHEMA
from pipelines.utils.env import get_required_env, load_env

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_CSV_PATH = Path("data/fec_sample.csv")
DEFAULT_TABLE_ID = "raw.fec_contributions"
DELIMITER = "|"
ENCODING = "utf-8"


# ---------------------------------------------------------------------------
# Core functions (small, testable, single responsibility)
# ---------------------------------------------------------------------------

def load_csv_to_dataframe(csv_path: Path) -> pd.DataFrame:
    """
    Read FEC CSV into a pandas DataFrame.
    Preserves all original FEC column names — no renaming.
    All fields read as strings initially to preserve raw values.
    TRANSACTION_AMT will be cast during BigQuery load via schema.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file not found at: {csv_path}\n"
            f"Run scripts/explore_fec.py to generate the sample."
        )

    df = pd.read_csv(
        csv_path,
        delimiter=DELIMITER,
        encoding=ENCODING,
        dtype=str,              # read all fields as string — BigQuery schema enforces types at load
        keep_default_na=False,  # preserve empty strings as empty, not NaN
    )

    print(f"  Loaded {len(df):,} rows from {csv_path}")
    return df


def add_load_date_column(df: pd.DataFrame, execution_date: date) -> pd.DataFrame:
    """
    Add _load_date column to dataframe.
    Every row in a single load gets the same execution date.
    This is the partition key — it determines which partition gets overwritten.
    """
    df = df.copy()
    df["_load_date"] = execution_date
    return df


def load_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    table_id: str,
    execution_date: date,
    client: bigquery.Client = None,
) -> int:
    """
    Load dataframe to BigQuery using partition overwrite.

    WRITE_TRUNCATE on a partitioned table overwrites ONLY the
    partition matching the execution_date — not the entire table.

    Returns: number of rows loaded
    """
    if client is None:
        client = bigquery.Client(project=project_id)

    # Ensure table exists with correct partition spec before loading
    ensure_table_exists(client, project_id, table_id)

    full_table_id = f"{project_id}.{table_id}${execution_date.strftime('%Y%m%d')}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(f["name"], f["type"], mode=f["mode"])
            for f in RAW_FEC_SCHEMA
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=0,  # dataframe has no header row when loaded
    )

    load_job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    load_job.result(timeout=300)  # 5 minute timeout — raises on failure or timeout

    rows_loaded = len(df)
    print(f"  Loaded {rows_loaded:,} rows → {full_table_id}")
    return rows_loaded


def count_rows_in_partition(
    client: bigquery.Client,
    project_id: str,
    table_id: str,
    execution_date: date,
) -> int:
    """
    Count rows in a specific date partition.
    Used to verify load and test idempotency.
    Note: uses f-string SQL — acceptable for internal verification queries only.
    Do not use this pattern for user-facing or staging SQL.
    """
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{project_id}.{table_id}`
        WHERE _load_date = DATE('{execution_date.isoformat()}')
    """
    result = list(client.query(query).result())
    return result[0]["row_count"]


def ensure_table_exists(
    client: bigquery.Client,
    project_id: str,
    table_id: str,
) -> None:
    """
    Create the partitioned BigQuery table if it does not exist.
    Safe to call multiple times — no-op if table already exists.
    """
    full_table_id = f"{project_id}.{table_id}"

    schema = [
        bigquery.SchemaField(f["name"], f["type"], mode=f["mode"])
        for f in RAW_FEC_SCHEMA
    ]

    table = bigquery.Table(full_table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="_load_date",
    )

    try:
        client.create_table(table)
        print(f"  Created table: {full_table_id}")
    except Exception:
        pass  # table already exists — continue


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load FEC sample CSV into BigQuery raw layer."
    )
    parser.add_argument(
        "--execution-date",
        required=True,
        help="Partition date for this load (YYYY-MM-DD). Example: 2025-01-01",
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help=f"Path to FEC CSV file (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--table-id",
        default=DEFAULT_TABLE_ID,
        help=f"BigQuery table ID (default: {DEFAULT_TABLE_ID})",
    )
    return parser.parse_args()


def main():
    load_env()

    args = parse_args()

    # Parse and validate execution date
    try:
        execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d").date()
    except ValueError:
        print(f"ERROR: Invalid date format '{args.execution_date}'. Use YYYY-MM-DD.")
        sys.exit(1)

    project_id = get_required_env("GCP_PROJECT_ID")

    print(f"\n{'='*60}")
    print("FEC RAW INGESTION")
    print(f"{'='*60}")
    print(f"  Execution date : {execution_date}")
    print(f"  Source file    : {args.csv_path}")
    print(f"  Target table   : {project_id}.{args.table_id}")
    print(f"{'='*60}\n")

    # Step 1 — load CSV
    df = load_csv_to_dataframe(args.csv_path)

    # Step 2 — add partition key
    df = add_load_date_column(df, execution_date)

    # Step 3 — load to BigQuery
    client = bigquery.Client(project=project_id)
    rows_loaded = load_to_bigquery(df, project_id, args.table_id, execution_date, client)

    # Step 4 — verify
    bq_count = count_rows_in_partition(client, project_id, args.table_id, execution_date)

    print(f"\n{'='*60}")
    print("INGESTION COMPLETE")
    print(f"{'='*60}")
    print(f"  Rows loaded    : {rows_loaded:,}")
    print(f"  BigQuery count : {bq_count:,}")
    print(f"  Status         : {'✅ MATCH' if rows_loaded == bq_count else '❌ MISMATCH'}")
    print(f"{'='*60}\n")

    if rows_loaded != bq_count:
        print("ERROR: Row count mismatch — investigate before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main()
