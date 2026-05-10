"""
Pipeline Run Logging Utility
Ticket 6.1: pipeline_run_log Metadata Table

Inserts one row into metadata.pipeline_run_log per call.
Append-only. No updates. No merges. No deletes.

Think of each call as leaving a breadcrumb:
  "Task X ran at time T, processed N rows, and PASSED."

Usage:
    from pipelines.utils.log_run import log_run

    log_run(
        run_id="2024-01-01-ingest-raw",
        execution_date="2024-01-01",
        task_name="ingest_raw",
        row_count_input=0,
        row_count_output=15264403,
        status="PASS",
    )
"""

from datetime import datetime, timezone

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from pipelines.utils.env import get_required_env, load_env

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOG_TABLE = "metadata.pipeline_run_log"
VALID_STATUSES = {"PASS", "FAIL"}


# ---------------------------------------------------------------------------
# Table management
# ---------------------------------------------------------------------------

def ensure_log_table_exists(
    client: bigquery.Client,
    project_id: str,
) -> None:
    """
    Create pipeline_run_log table if it does not exist.
    Append-only design: no partitioning, no clustering for MVP.
    Safe to call multiple times.
    """
    # Ensure metadata dataset exists first
    dataset_id = f"{project_id}.metadata"
    try:
        client.create_dataset(dataset_id)
        print(f"  Created dataset: {dataset_id}")
    except Conflict:
        pass

    full_table_id = f"{project_id}.{LOG_TABLE}"

    schema = [
        bigquery.SchemaField("run_id",           "STRING",    "REQUIRED"),
        bigquery.SchemaField("execution_date",   "DATE",      "REQUIRED"),
        bigquery.SchemaField("task_name",        "STRING",    "REQUIRED"),
        bigquery.SchemaField("row_count_input",  "INTEGER",   "NULLABLE"),
        bigquery.SchemaField("row_count_output", "INTEGER",   "NULLABLE"),
        bigquery.SchemaField("status",           "STRING",    "REQUIRED"),
        bigquery.SchemaField("timestamp",        "TIMESTAMP", "REQUIRED"),
    ]

    table = bigquery.Table(full_table_id, schema=schema)

    try:
        client.create_table(table)
        print(f"  Created table: {full_table_id}")
    except Conflict:
        pass  # table already exists


# ---------------------------------------------------------------------------
# Core logging function
# ---------------------------------------------------------------------------

def validate_status(status: str) -> str:
    """
    Validate that status is exactly PASS or FAIL.
    Raises ValueError for any other value.
    """
    if status not in VALID_STATUSES:
        raise ValueError(
            f"Invalid status '{status}'. Must be one of: {VALID_STATUSES}"
        )
    return status


def log_run(
    run_id: str,
    execution_date: str,
    task_name: str,
    row_count_input: int,
    row_count_output: int,
    status: str,
    client: bigquery.Client = None,
    project_id: str = None,
) -> None:
    """
    Insert one log entry into pipeline_run_log.

    Append-only. Every call adds one row.
    No update logic. No merge logic. No overwrite logic.

    Args:
        run_id:           Unique identifier for this pipeline run.
                          Convention: "{execution_date}-{task_name}"
        execution_date:   Logical date being processed (YYYY-MM-DD string).
        task_name:        Name of the pipeline task.
        row_count_input:  Rows available at task start (0 if not applicable).
        row_count_output: Rows written or merged at task end.
        status:           "PASS" or "FAIL" only.
        client:           Optional BigQuery client (created if not provided).
        project_id:       Optional project ID (read from env if not provided).
    """
    validate_status(status)

    load_env()

    if project_id is None:
        project_id = get_required_env("GCP_PROJECT_ID")

    if client is None:
        client = bigquery.Client(project=project_id)

    ensure_log_table_exists(client, project_id)

    full_table_id = f"{project_id}.{LOG_TABLE}"

    if row_count_input is None:
        row_count_input = 0
    if row_count_output is None:
        row_count_output = 0

    rows = [{
        "run_id":           run_id,
        "execution_date":   execution_date,
        "task_name":        task_name,
        "row_count_input":  row_count_input,
        "row_count_output": row_count_output,
        "status":           status,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }]

    errors = client.insert_rows_json(full_table_id, rows)

    if errors:
        raise RuntimeError(
            f"Failed to insert log entry into {full_table_id}: {errors}"
        )
