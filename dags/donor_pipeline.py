"""
Donor Intelligence Platform: Full Pipeline DAG
Ticket 6.2: Full Pipeline DAG

Orchestrates the complete pipeline in strict fail-fast order:

    ingest_raw
        |
    check_raw          <- stops here if raw partition is empty
        |
    build_staging
        |
    check_staging      <- stops here if staging is empty
        |
    build_identity_layer
        |
    check_identity     <- stops here if dim_donors is empty
        |
    build_mart
        |
    check_mart         <- stops here if mart is empty or has duplicates

Each check task raises an exception on failure.
Airflow marks downstream tasks as skipped automatically.
No downstream task runs after a failed check.

Idempotency:
    Raw layer: partition overwrite
    Staging:   MERGE on SUB_ID
    Identity:  MERGE on SUB_ID
    Mart:      full rebuild (CREATE OR REPLACE TABLE)

Running this DAG twice with the same execution_date produces
identical outputs with no duplicate records.

pipeline_run_log receives one entry per task per run.
"""

import uuid
from datetime import date, datetime
from pathlib import Path

from airflow.operators.python import PythonOperator
from google.cloud import bigquery

from airflow import DAG
from pipelines.identity.build_identity import run_identity_resolution
from pipelines.ingest.load_raw_fec import ensure_table_exists, load_csv_chunked
from pipelines.marts.build_mart import build_mart
from pipelines.quality.check_identity import run_identity_quality_checks
from pipelines.quality.check_mart import run_mart_quality_checks
from pipelines.quality.check_raw import run_raw_quality_checks
from pipelines.quality.check_staging import run_staging_quality_checks
from pipelines.staging.build_staging import run_staging_chunked
from pipelines.utils.env import get_required_env, load_env
from pipelines.utils.log_run import log_run

# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "retries": 0,                          # fail fast, no automatic retries
}

# ---------------------------------------------------------------------------
# Helper: run a task with logging
# ---------------------------------------------------------------------------

def run_with_logging(
    task_fn,
    task_name: str,
    execution_date: str,
    row_count_input: int = 0,
) -> int:
    load_env()
    run_id = f"{execution_date}-{task_name}-{str(uuid.uuid4())[:8]}"
    status = "FAIL"
    row_count_output = 0

    try:
        result = task_fn()
        row_count_output = result if isinstance(result, int) else 0
        status = "PASS"
        return row_count_output
    except Exception:
        raise
    finally:
        try:
            log_run(
                run_id=run_id,
                execution_date=execution_date,
                task_name=task_name,
                row_count_input=row_count_input,
                row_count_output=row_count_output,
                status=status,
            )
        except Exception as log_err:
            print(f"WARNING: Failed to write to pipeline_run_log: {log_err}")


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def task_ingest_raw(**context) -> None:
    execution_date = context["ds"]
    csv_path = Path(context["params"]["csv_path"])
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)
    exec_date = date.fromisoformat(execution_date)  # convert string to date

    def _run():
        ensure_table_exists(client, project_id, "raw.fec_contributions")
        return load_csv_chunked(
            csv_path=csv_path,
            project_id=project_id,
            table_id="raw.fec_contributions",
            execution_date=exec_date,
            client=client,
            has_header=csv_path.suffix == ".csv",  # True for .csv, False for .txt
        )

    run_with_logging(_run, "ingest_raw", execution_date)


def task_check_raw(**context) -> None:
    execution_date = context["ds"]
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    def _run():
        return run_raw_quality_checks(client, project_id, execution_date)

    run_with_logging(_run, "check_raw", execution_date)


def task_build_staging(**context) -> None:
    execution_date = context["ds"]
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")

    client = bigquery.Client(project=project_id)
    exec_date = date.fromisoformat(execution_date)

    def _run():
        return run_staging_chunked(client, project_id, exec_date)

    run_with_logging(_run, "build_staging", execution_date)


def task_check_staging(**context) -> None:
    execution_date = context["ds"]
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    def _run():
        raw_count, staging_count = run_staging_quality_checks(
            client, project_id, execution_date
        )
        return staging_count

    run_with_logging(_run, "check_staging", execution_date)


def task_build_identity_layer(**context) -> None:
    execution_date = context["ds"]
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")

    client = bigquery.Client(project=project_id)
    exec_date = date.fromisoformat(execution_date)

    def _run():
        return run_identity_resolution(client, project_id, execution_date=exec_date)

    run_with_logging(_run, "build_identity_layer", execution_date)


def task_check_identity(**context) -> None:
    execution_date = context["ds"]
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    def _run():
        staging, donors, unresolved = run_identity_quality_checks(
            client, project_id, execution_date
        )
        return donors

    run_with_logging(_run, "check_identity", execution_date)


def task_build_mart(**context) -> None:
    execution_date = context["ds"]
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")

    client = bigquery.Client(project=project_id)

    def _run():
        return build_mart(client, project_id)

    run_with_logging(_run, "build_mart", execution_date)


def task_check_mart(**context) -> None:
    execution_date = context["ds"]
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    def _run():
        return run_mart_quality_checks(client, project_id)

    run_with_logging(_run, "check_mart", execution_date)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="donor_pipeline",
    default_args=default_args,
    description="Donor Intelligence Platform: full pipeline from raw ingestion to analytics mart",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "csv_path": "data/itcont_2024.txt",
    },
    tags=["donor-intelligence", "fec", "production"],
) as dag:

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=task_ingest_raw,
    )

    check_raw_task = PythonOperator(
        task_id="check_raw",
        python_callable=task_check_raw,
    )

    build_staging = PythonOperator(
        task_id="build_staging",
        python_callable=task_build_staging,
    )

    check_staging_task = PythonOperator(
        task_id="check_staging",
        python_callable=task_check_staging,
    )

    build_identity_layer = PythonOperator(
        task_id="build_identity_layer",
        python_callable=task_build_identity_layer,
    )

    check_identity_task = PythonOperator(
        task_id="check_identity",
        python_callable=task_check_identity,
    )

    build_mart_task = PythonOperator(
        task_id="build_mart",
        python_callable=task_build_mart,
    )

    check_mart_task = PythonOperator(
        task_id="check_mart",
        python_callable=task_check_mart,
    )

    # Strict fail-fast dependency chain
    (
        ingest_raw
        >> check_raw_task
        >> build_staging
        >> check_staging_task
        >> build_identity_layer
        >> check_identity_task
        >> build_mart_task
        >> check_mart_task
    )
