"""
Mart Layer Quality Checks
Ticket 7.4: Mart Layer Quality Checks

Three checks run immediately after mart build:
    1. Grain check: COUNT(*) == COUNT(DISTINCT donor_id)
    2. Null engagement score: zero nulls allowed
    3. Row count consistency: mart == dim_donors with valid contribution dates

Architecture note:
    mart_donor_summary excludes donors whose contributions all have null
    contribution_date values. The row count check compares mart against
    dim_donors donors with at least one non-null contribution_date.

    This was discovered during implementation: 2 donors in dim_donors
    (filipiak karolina ZIP 95054 and pierson ryan ZIP 95054) have 100%
    null contribution_date values and are correctly excluded from the mart.

    Real data:
        mart_donor_summary:  3,160,102 rows
        eligible dim_donors: 3,160,102 (exact match)
        excluded donors:     2 (all contributions have null dates)
        null engagement:     0

Note: logging to pipeline_run_log is the caller's responsibility.
When called from the Airflow DAG via run_with_logging, PASS and
FAIL are logged automatically. When called manually, wrap with
log_run() if an audit trail is required.
"""

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MART_TABLE = "marts.mart_donor_summary"
DIM_DONORS_TABLE = "core.dim_donors"
STAGING_TABLE = "staging.stg_contributions"


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_grain(
    client: bigquery.Client,
    project_id: str,
) -> tuple[int, int]:
    """
    Verify mart has exactly one row per donor_id.
    Returns (total_count, distinct_count) on success.
    Raises ValueError if duplicates exist or mart is empty.

    Grain: one row per donor_id.
    Duplicates indicate a mart rebuild failure.
    """
    query = f"""
        SELECT
            COUNT(*) as total_count,
            COUNT(DISTINCT donor_id) as distinct_count
        FROM `{project_id}.{MART_TABLE}`
    """
    row = list(client.query(query).result())[0]
    total_count = row["total_count"]
    distinct_count = row["distinct_count"]

    if total_count == 0:
        raise ValueError(
            "check_mart grain check FAILED: mart_donor_summary is empty"
        )

    if total_count != distinct_count:
        raise ValueError(
            f"check_mart grain check FAILED: "
            f"{total_count - distinct_count:,} duplicate donor_ids found. "
            f"total={total_count:,} distinct={distinct_count:,}"
        )

    print(f"  check_mart grain: {total_count:,} rows, {distinct_count:,} unique donors -- PASS")
    return total_count, distinct_count


def check_null_engagement_scores(
    client: bigquery.Client,
    project_id: str,
) -> None:
    """
    Verify no null engagement_score values exist in mart.
    Raises ValueError if any nulls exist or mart is empty.

    engagement_score = 0.4*recency + 0.3*frequency + 0.3*monetary.
    A null score indicates a scoring logic failure.
    Real data: zero null scores confirmed.
    """
    query = f"""
        SELECT
            COUNT(*) as total,
            COUNTIF(engagement_score IS NULL) as null_count
        FROM `{project_id}.{MART_TABLE}`
    """
    row = list(client.query(query).result())[0]
    total = row["total"]
    null_count = row["null_count"]

    if total == 0:
        raise ValueError(
            "check_mart null score check FAILED: mart_donor_summary is empty"
        )

    if null_count > 0:
        raise ValueError(
            f"check_mart null score check FAILED: "
            f"{null_count:,} null engagement_score values found"
        )

    print(f"  check_mart null scores: {total:,} rows, zero nulls -- PASS")


def check_row_count_consistency(
    client: bigquery.Client,
    project_id: str,
) -> tuple[int, int]:
    """
    Verify mart row count matches eligible dim_donors count.
    Returns (mart_count, eligible_count) on success.
    Raises ValueError if counts do not match exactly.

    Eligible donors: those with at least one non-null contribution_date
    in staging. Donors whose contributions all have null dates are
    correctly excluded from the mart by the WHERE contribution_date
    IS NOT NULL filter in mart_donor_summary.sql.

    Real data: 2 donors excluded (all contributions have null dates).
    """
    query = f"""
        SELECT
            m.mart_count,
            e.eligible_count
        FROM
            (SELECT COUNT(*) as mart_count
             FROM `{project_id}.{MART_TABLE}`) m,
            (SELECT COUNT(DISTINCT d.donor_id) as eligible_count
             FROM `{project_id}.{DIM_DONORS_TABLE}` d
             JOIN `{project_id}.{STAGING_TABLE}` s
                 ON d.sub_id = s.sub_id
             WHERE s.contribution_date IS NOT NULL) e
    """
    row = list(client.query(query).result())[0]
    mart_count = row["mart_count"]
    eligible_count = row["eligible_count"]

    if mart_count == 0:
        raise ValueError(
            "check_mart row count check FAILED: mart_donor_summary is empty"
        )

    if mart_count != eligible_count:
        raise ValueError(
            f"check_mart row count check FAILED: "
            f"mart={mart_count:,} != eligible_donors={eligible_count:,}"
        )

    print(
        f"  check_mart row count: "
        f"mart={mart_count:,} == eligible_donors={eligible_count:,} -- PASS"
    )
    return mart_count, eligible_count


# ---------------------------------------------------------------------------
# Full quality check runner
# ---------------------------------------------------------------------------

def run_mart_quality_checks(
    client: bigquery.Client,
    project_id: str,
) -> int:
    """
    Run all three mart quality checks in order.
    Stops at first failure. Returns mart row count on success.

    Checks run in this order:
        1. Grain check (catches duplicate donor_ids first)
        2. Null engagement scores (catches scoring failures)
        3. Row count consistency (catches missing donors)

    Note: no execution_date parameter because mart_donor_summary is a
    full rebuild with no date partitioning. Every run replaces the
    entire table. All checks operate on the complete mart.

    Note: logging to pipeline_run_log is the caller's responsibility.
    When called from the Airflow DAG via run_with_logging, PASS and
    FAIL are logged automatically. When called manually, wrap with
    log_run() if an audit trail is required.
    """
    mart_count, _ = check_grain(client, project_id)
    check_null_engagement_scores(client, project_id)
    check_row_count_consistency(client, project_id)

    print("  check_mart: all quality checks PASSED")
    return mart_count
