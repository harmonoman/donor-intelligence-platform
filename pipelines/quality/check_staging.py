"""
Staging Layer Quality Checks

Three checks run immediately after staging build:
    1. Row count consistency: staging must be 95-100% of raw row count
    2. Null rates: donor_name_normalized and zip_normalized below thresholds
    3. Duplicate MERGE key: no duplicate sub_id values allowed

Why these checks:
    Row count consistency catches over-filtering or under-filtering
    in the ENTITY_TP = IND filter. The 2024 cycle confirmed 99.94%
    pass-through rate, so 95% is a conservative lower bound.

    Null rates catch normalization failures. Real data shows 0% nulls
    in both fields after staging. Thresholds are set generously to
    accommodate natural variation across FEC filing periods.

    Duplicate sub_id detection catches MERGE failures. SUB_ID is the
    primary MERGE key confirmed 100% populated across 28M rows.
    Any duplicate indicates a pipeline integrity failure.

Note: logging to pipeline_run_log is the caller's responsibility.
When called from the Airflow DAG via run_with_logging, PASS and
FAIL are logged automatically. When called manually, wrap with
log_run() if an audit trail is required.
"""

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RAW_TABLE = "raw.fec_contributions"
STAGING_TABLE = "staging.stg_contributions"
MERGE_KEY = "sub_id"

# Row count threshold: staging must be between 95% and 100% of raw
# Real data: 99.94% pass-through rate (2024 cycle)
# Lower bound set at 95% to accommodate variation across filing periods
ROW_COUNT_MIN_PCT = 95.0
ROW_COUNT_MAX_PCT = 100.0  # staging can never exceed raw (filter only removes)

# Null rate thresholds
# Real data: 0% nulls in both fields after staging
# Thresholds set generously to accommodate edge cases
NULL_THRESHOLD_NAME = 1.0   # donor_name_normalized must be under 1% null
NULL_THRESHOLD_ZIP = 5.0    # zip_normalized must be under 5% null


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_row_count_consistency(
    client: bigquery.Client,
    project_id: str,
    execution_date: str,
) -> tuple[int, int]:
    """
    Verify staging row count is within acceptable range of raw row count.
    Returns (raw_count, staging_count) on success.
    Raises ValueError if outside threshold or if either count is zero.

    Threshold: staging must be 95-100% of raw.
    Real data: 99.94% (filter removes 0.06% non-IND records).
    """
    raw_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.{RAW_TABLE}`
        WHERE _load_date = DATE('{execution_date}')
    """
    staging_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.{STAGING_TABLE}`
        WHERE _load_date = DATE('{execution_date}')
    """

    raw_count = list(client.query(raw_query).result())[0]["cnt"]
    staging_count = list(client.query(staging_query).result())[0]["cnt"]

    if raw_count == 0:
        raise ValueError(
            f"row count consistency check FAILED: "
            f"no rows in raw partition for {execution_date}"
        )

    if staging_count == 0:
        raise ValueError(
            f"row count consistency check FAILED: "
            f"no rows in staging partition for {execution_date}"
        )

    pct = staging_count * 100.0 / raw_count

    if pct < ROW_COUNT_MIN_PCT or pct > ROW_COUNT_MAX_PCT:
        raise ValueError(
            f"row count consistency check FAILED: "
            f"staging is {pct:.2f}% of raw "
            f"(expected {ROW_COUNT_MIN_PCT}% to {ROW_COUNT_MAX_PCT}%). "
            f"raw={raw_count:,} staging={staging_count:,}"
        )

    print(
        f"  check_staging row count: "
        f"staging={staging_count:,} ({pct:.2f}% of raw={raw_count:,}) -- PASS"
    )
    return raw_count, staging_count


def check_null_rates(
    client: bigquery.Client,
    project_id: str,
    execution_date: str,
) -> None:
    """
    Verify null rates in critical staging fields are within thresholds.
    Raises ValueError if any field exceeds its threshold.

    Fields checked:
        donor_name_normalized: must be under 1% null
        zip_normalized: must be under 5% null

    Real data: 0% nulls in both fields (2024 cycle, 15.2M rows).
    """
    query = f"""
        SELECT
            COUNT(*) as total,
            COUNTIF(donor_name_normalized IS NULL) as null_name,
            COUNTIF(zip_normalized IS NULL) as null_zip
        FROM `{project_id}.{STAGING_TABLE}`
        WHERE _load_date = DATE('{execution_date}')
    """
    row = list(client.query(query).result())[0]
    total = row["total"]

    if total == 0:
        raise ValueError(
            f"check_staging null rate check FAILED: "
            f"no rows in staging partition for {execution_date}"
        )

    null_name_pct = row["null_name"] * 100.0 / total
    null_zip_pct = row["null_zip"] * 100.0 / total

    if null_name_pct >= NULL_THRESHOLD_NAME:
        raise ValueError(
            f"check_staging null rate check FAILED: "
            f"donor_name_normalized is {null_name_pct:.2f}% null "
            f"(threshold: {NULL_THRESHOLD_NAME}%)"
        )

    if null_zip_pct >= NULL_THRESHOLD_ZIP:
        raise ValueError(
            f"check_staging null rate check FAILED: "
            f"zip_normalized is {null_zip_pct:.2f}% null "
            f"(threshold: {NULL_THRESHOLD_ZIP}%)"
        )

    print(
        f"  check_staging null rates: "
        f"name={null_name_pct:.4f}% zip={null_zip_pct:.4f}% -- PASS"
    )


def check_duplicate_merge_keys(
    client: bigquery.Client,
    project_id: str,
    execution_date: str,
) -> None:
    """
    Verify no duplicate sub_id values exist in staging partition.
    Raises ValueError if any duplicates are found.

    SUB_ID is the primary MERGE key confirmed 100% populated across
    28M rows. Any duplicate indicates a pipeline integrity failure.
    """
    query = f"""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT {MERGE_KEY}, COUNT(*) as cnt
            FROM `{project_id}.{STAGING_TABLE}`
            WHERE _load_date = DATE('{execution_date}')
            GROUP BY {MERGE_KEY}
            HAVING cnt > 1
        )
    """
    dup_count = list(client.query(query).result())[0]["dup_count"]

    if dup_count > 0:
        raise ValueError(
            f"check_staging duplicate key check FAILED: "
            f"{dup_count:,} duplicate {MERGE_KEY} values found "
            f"in staging partition for {execution_date}"
        )

    print("  check_staging duplicate keys: no duplicates found -- PASS")


# ---------------------------------------------------------------------------
# Full quality check runner
# ---------------------------------------------------------------------------

def run_staging_quality_checks(
    client: bigquery.Client,
    project_id: str,
    execution_date: str,
) -> tuple[int, int]:
    """
    Run all three staging quality checks in order.
    Stops at first failure. Returns (raw_count, staging_count) on success.

    Checks run in this order:
        1. Row count consistency (catches filter failures first)
        2. Null rates (catches normalization failures)
        3. Duplicate MERGE key (catches data integrity failures)

    Note: logging to pipeline_run_log is the caller's responsibility.
    When called from the Airflow DAG via run_with_logging, PASS and
    FAIL are logged automatically. When called manually, wrap with
    log_run() if an audit trail is required.
    """
    raw_count, staging_count = check_row_count_consistency(
        client, project_id, execution_date
    )
    check_null_rates(client, project_id, execution_date)
    check_duplicate_merge_keys(client, project_id, execution_date)

    print(f"  check_staging: all quality checks PASSED for {execution_date}")
    return raw_count, staging_count
