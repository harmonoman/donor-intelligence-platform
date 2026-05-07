"""
Mart Build Script
Ticket 5.2: mart_donor_summary Build

Builds the analytics mart from identity and staging layers.

Pipeline flow:
    core.dim_donors + staging.stg_contributions
    -> donor-level aggregation
    -> RFM scoring
    -> marts.mart_donor_summary

Why full rebuild, not incremental:
    The mart is an analytical summary. Rebuilding it from scratch
    on every run is simple, correct, and idempotent. With 3M donors
    this runs in under 5 minutes in BigQuery. Incremental mart logic
    adds complexity without meaningful benefit at this scale.

Why CURRENT_DATE() is NOT used for recency:
    FEC data is historical. Using CURRENT_DATE() would make every
    donor appear lapsed by 2026. The mart uses MAX(last_donation_date)
    as the reference date so recency reflects the data, not the calendar.
"""

import argparse
from pathlib import Path

from google.cloud import bigquery

from pipelines.utils.env import get_required_env, load_env

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MART_TABLE = "marts.mart_donor_summary"
MART_SQL_PATH = Path("sql/marts/mart_donor_summary.sql")


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def build_mart(
    client: bigquery.Client,
    project_id: str,
) -> int:
    """
    Execute mart SQL and return row count.
    Full rebuild on every run. WRITE_TRUNCATE guarantees idempotency.
    """
    if not MART_SQL_PATH.exists():
        raise FileNotFoundError(f"Mart SQL not found at: {MART_SQL_PATH}")

    sql = MART_SQL_PATH.read_text()

    # Replace placeholder project with actual project
    sql = sql.replace("{PROJECT_ID}", project_id)

    print(f"  Building {MART_TABLE}...")
    client.query(sql).result(timeout=600)  # 10 min timeout for large dataset

    count = count_mart_rows(client, project_id)
    print(f"  Mart built: {count:,} donor rows")
    return count


def count_mart_rows(
    client: bigquery.Client,
    project_id: str,
) -> int:
    """Count rows in mart_donor_summary."""
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{project_id}.{MART_TABLE}`
    """
    return list(client.query(query).result())[0]["row_count"]


def validate_no_duplicate_donor_ids(
    client: bigquery.Client,
    project_id: str,
) -> None:
    """
    Verify mart has exactly one row per donor_id.
    Fails loudly if duplicates exist.
    """
    query = f"""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT donor_id, COUNT(*) as cnt
            FROM `{project_id}.{MART_TABLE}`
            GROUP BY donor_id
            HAVING cnt > 1
        )
    """
    duplicate_count = list(client.query(query).result())[0]["duplicate_count"]
    if duplicate_count > 0:
        raise ValueError(
            f"VALIDATION FAILED: {duplicate_count:,} duplicate donor_ids in mart"
        )
    print("  Validation passed: no duplicate donor_ids")


def validate_no_null_engagement_scores(
    client: bigquery.Client,
    project_id: str,
) -> None:
    """
    Verify all rows have a non-null engagement score.
    """
    query = f"""
        SELECT COUNT(*) as null_count
        FROM `{project_id}.{MART_TABLE}`
        WHERE engagement_score IS NULL
    """
    null_count = list(client.query(query).result())[0]["null_count"]
    if null_count > 0:
        raise ValueError(
            f"VALIDATION FAILED: {null_count:,} rows with null engagement_score"
        )
    print("  Validation passed: no null engagement scores")


def log_score_distribution(
    client: bigquery.Client,
    project_id: str,
) -> None:
    """Log engagement score distribution for operational visibility."""
    query = f"""
        SELECT
            CASE
                WHEN engagement_score >= 2.5 THEN 'high (2.5+)'
                WHEN engagement_score >= 1.8 THEN 'medium (1.8-2.4)'
                ELSE 'low (under 1.8)'
            END AS engagement_tier,
            COUNT(*) as donor_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
        FROM `{project_id}.{MART_TABLE}`
        GROUP BY engagement_tier
        ORDER BY MIN(engagement_score) DESC
    """
    print("  Engagement score distribution:")
    for row in client.query(query).result():
        print(f"    {row['engagement_tier']}: {row['donor_count']:,} ({row['pct']}%)")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Build marts.mart_donor_summary from identity and staging layers."
    )
    parser.add_argument(
        "--execution-date",
        required=False,
        default=None,
        help="Execution date (YYYY-MM-DD). Logged only. Mart is always a full rebuild.",
    )
    return parser.parse_args()


def main():
    load_env()
    args = parse_args()

    project_id = get_required_env("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    print(f"\n{'='*60}")
    print("MART BUILD")
    print(f"{'='*60}")
    print(f"  Target table   : {project_id}.{MART_TABLE}")
    print("  Mode           : full rebuild")
    if args.execution_date:
        print(f"  Execution date : {args.execution_date}")
    print(f"{'='*60}\n")

    count = build_mart(client, project_id)

    validate_no_duplicate_donor_ids(client, project_id)
    validate_no_null_engagement_scores(client, project_id)
    log_score_distribution(client, project_id)

    print(f"\n{'='*60}")
    print("MART BUILD COMPLETE")
    print(f"{'='*60}")
    print(f"  Donors in mart : {count:,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
