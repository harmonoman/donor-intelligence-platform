"""
Identity Layer Quality Checks
Identity Layer Quality Checks

Three checks run immediately after identity resolution:
    1. sub_id uniqueness: each sub_id must appear exactly once in dim_donors
    2. Row count reconciliation: staging == dim_donors + dim_donors_unresolved
    3. No null donor_id: every dim_donors row must have a donor_id

Architecture note:
    donor_id is NOT unique per row in dim_donors. Multiple contributions
    from the same donor share one donor_id (same canonical key = same donor).
    sub_id IS unique per row and is the correct uniqueness constraint.

    Real data (2024 partition):
        15,255,256 total rows
         2,375,123 unique donor_ids
        15,255,256 unique sub_ids
                 0 null donor_ids
                 0 unresolved records

Note: logging to pipeline_run_log is the caller's responsibility.
When called from the Airflow DAG via run_with_logging, PASS and
FAIL are logged automatically. When called manually, wrap with
log_run() if an audit trail is required.
"""

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STAGING_TABLE = "staging.stg_contributions"
DIM_DONORS_TABLE = "core.dim_donors"
UNRESOLVED_TABLE = "core.dim_donors_unresolved"


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_sub_id_uniqueness(
    client: bigquery.Client,
    project_id: str,
    execution_date: str,
) -> None:
    """
    Verify each sub_id appears exactly once in dim_donors for this partition.
    Raises ValueError if duplicates exist or if partition is empty.

    sub_id is the MERGE key for dim_donors. Duplicates indicate a
    MERGE failure or data integrity problem in identity resolution.

    Note: donor_id is intentionally NOT unique per row. Multiple
    contributions from the same donor share one donor_id by design.
    """
    count_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.{DIM_DONORS_TABLE}`
        WHERE _load_date = DATE('{execution_date}')
    """
    count = list(client.query(count_query).result())[0]["cnt"]

    if count == 0:
        raise ValueError(
            f"sub_id uniqueness check FAILED: "
            f"no rows in dim_donors for {execution_date}"
        )

    dup_query = f"""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT sub_id, COUNT(*) as cnt
            FROM `{project_id}.{DIM_DONORS_TABLE}`
            WHERE _load_date = DATE('{execution_date}')
            GROUP BY sub_id
            HAVING cnt > 1
        )
    """
    dup_count = list(client.query(dup_query).result())[0]["dup_count"]

    if dup_count > 0:
        raise ValueError(
            f"sub_id uniqueness check FAILED: "
            f"{dup_count:,} duplicate sub_id values found "
            f"in dim_donors for {execution_date}"
        )

    print(f"  check_identity sub_id uniqueness: {count:,} rows, no duplicates -- PASS")


def check_row_count_reconciliation(
    client: bigquery.Client,
    project_id: str,
    execution_date: str,
) -> tuple[int, int, int]:
    """
    Verify staging row count equals dim_donors + dim_donors_unresolved.
    Returns (staging_count, donors_count, unresolved_count) on success.
    Raises ValueError if counts do not reconcile exactly.

    This is a strict equality check. No tolerance.
    Any discrepancy indicates records were silently lost or duplicated
    during identity resolution.
    """
    query = f"""
        SELECT
            s.staging_count,
            d.donors_count,
            u.unresolved_count
        FROM
            (SELECT COUNT(*) as staging_count
             FROM `{project_id}.{STAGING_TABLE}`
             WHERE _load_date = DATE('{execution_date}')) s,
            (SELECT COUNT(*) as donors_count
             FROM `{project_id}.{DIM_DONORS_TABLE}`
             WHERE _load_date = DATE('{execution_date}')) d,
            (SELECT COUNT(*) as unresolved_count
             FROM `{project_id}.{UNRESOLVED_TABLE}`
             WHERE _load_date = DATE('{execution_date}')) u
    """
    row = list(client.query(query).result())[0]
    staging_count = row["staging_count"]
    donors_count = row["donors_count"]
    unresolved_count = row["unresolved_count"]

    if staging_count == 0:
        raise ValueError(
            f"row count reconciliation check FAILED: "
            f"no rows in staging for {execution_date}"
        )

    total_identity = donors_count + unresolved_count

    if staging_count != total_identity:
        raise ValueError(
            f"row count reconciliation check FAILED: "
            f"staging={staging_count:,} != "
            f"dim_donors={donors_count:,} + "
            f"dim_donors_unresolved={unresolved_count:,} "
            f"({total_identity:,}) for {execution_date}"
        )

    print(
        f"  check_identity row count: "
        f"staging={staging_count:,} == "
        f"donors={donors_count:,} + unresolved={unresolved_count:,} -- PASS"
    )
    return staging_count, donors_count, unresolved_count


def check_donor_id_not_null(
    client: bigquery.Client,
    project_id: str,
    execution_date: str,
) -> None:
    """
    Verify no null donor_id values exist in dim_donors for this partition.
    Raises ValueError if any nulls exist or if partition is empty.

    donor_id is generated via MD5 hash of the canonical key.
    A null donor_id indicates a failure in the hashing logic.
    Real data: zero null donor_ids confirmed across all partitions.
    """
    query = f"""
        SELECT
            COUNT(*) as total,
            COUNTIF(donor_id IS NULL) as null_count
        FROM `{project_id}.{DIM_DONORS_TABLE}`
        WHERE _load_date = DATE('{execution_date}')
    """
    row = list(client.query(query).result())[0]
    total = row["total"]
    null_count = row["null_count"]

    if total == 0:
        raise ValueError(
            f"null donor_id check FAILED: "
            f"no rows in dim_donors for {execution_date}"
        )

    if null_count > 0:
        raise ValueError(
            f"null donor_id check FAILED: "
            f"{null_count:,} null donor_id values found "
            f"in dim_donors for {execution_date}"
        )

    print(f"  check_identity null donor_id: {total:,} rows, zero nulls -- PASS")


# ---------------------------------------------------------------------------
# Full quality check runner
# ---------------------------------------------------------------------------

def run_identity_quality_checks(
    client: bigquery.Client,
    project_id: str,
    execution_date: str,
) -> tuple[int, int, int]:
    """
    Run all three identity quality checks in order.
    Stops at first failure.
    Returns (staging_count, donors_count, unresolved_count) on success.

    Checks run in this order:
        1. sub_id uniqueness (catches MERGE failures first)
        2. Row count reconciliation (catches silent record loss)
        3. Null donor_id (catches hashing failures)

    Note: logging to pipeline_run_log is the caller's responsibility.
    When called from the Airflow DAG via run_with_logging, PASS and
    FAIL are logged automatically. When called manually, wrap with
    log_run() if an audit trail is required.
    """
    check_sub_id_uniqueness(client, project_id, execution_date)
    staging, donors, unresolved = check_row_count_reconciliation(
        client, project_id, execution_date
    )
    check_donor_id_not_null(client, project_id, execution_date)

    print(f"  check_identity: all quality checks PASSED for {execution_date}")
    return staging, donors, unresolved
