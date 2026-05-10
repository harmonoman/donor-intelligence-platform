"""
Pipeline Validation Checks
Ticket 6.2: Full Pipeline DAG

Lightweight validation functions called by check tasks in the DAG.
Each function raises an exception on failure. No silent failures.

These are not complex data quality rules. They are basic sanity checks:
- Did the task produce any rows?
- Are critical fields populated?
- Is the mart grain intact?

Think of these as the inspection checkpoints on the factory assembly line.
"""

from google.cloud import bigquery

from pipelines.utils.env import get_required_env, load_env


def get_client() -> tuple[bigquery.Client, str]:
    """Return a BigQuery client and project_id."""
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)
    return client, project_id


def check_raw(execution_date: str) -> int:
    """
    Validate raw partition for execution_date has rows.
    Returns row count on success. Raises on failure.
    """
    client, project_id = get_client()
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.raw.fec_contributions`
        WHERE _load_date = DATE('{execution_date}')
    """
    count = list(client.query(query).result())[0]["cnt"]
    if count == 0:
        raise ValueError(
            f"check_raw FAILED: no rows in raw partition for {execution_date}"
        )
    print(f"  check_raw PASSED: {count:,} rows in raw partition {execution_date}")
    return count


def check_staging(execution_date: str) -> int:
    """
    Validate staging has rows for execution_date.
    Returns row count on success. Raises on failure.
    """
    client, project_id = get_client()
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.staging.stg_contributions`
        WHERE _load_date = DATE('{execution_date}')
    """
    count = list(client.query(query).result())[0]["cnt"]
    if count == 0:
        raise ValueError(
            f"check_staging FAILED: no rows in staging for {execution_date}"
        )
    print(f"  check_staging PASSED: {count:,} rows in staging {execution_date}")
    return count


def check_identity(execution_date: str) -> int:
    """
    Validate dim_donors has rows for execution_date.
    Returns row count on success. Raises on failure.
    """
    client, project_id = get_client()
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.core.dim_donors`
        WHERE _load_date = DATE('{execution_date}')
    """
    count = list(client.query(query).result())[0]["cnt"]
    if count == 0:
        raise ValueError(
            f"check_identity FAILED: no rows in dim_donors for {execution_date}"
        )
    print(f"  check_identity PASSED: {count:,} rows in dim_donors {execution_date}")
    return count


def check_mart() -> int:
    client, project_id = get_client()

    count_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.marts.mart_donor_summary`
    """
    count = list(client.query(count_query).result())[0]["cnt"]
    if count == 0:
        raise ValueError("check_mart FAILED: mart_donor_summary is empty")

    dup_query = f"""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT donor_id, COUNT(*) as cnt
            FROM `{project_id}.marts.mart_donor_summary`
            GROUP BY donor_id
            HAVING cnt > 1
        )
    """
    dup_count = list(client.query(dup_query).result())[0]["dup_count"]
    if dup_count > 0:
        raise ValueError(
            f"check_mart FAILED: {dup_count:,} duplicate donor_ids found"
        )

    null_query = f"""
        SELECT COUNT(*) as null_count
        FROM `{project_id}.marts.mart_donor_summary`
        WHERE engagement_score IS NULL
    """
    null_count = list(client.query(null_query).result())[0]["null_count"]
    if null_count > 0:
        raise ValueError(
            f"check_mart FAILED: {null_count:,} rows with null engagement_score"
        )

    print(f"  check_mart PASSED: {count:,} donors, no duplicates, no null scores")
    return count
