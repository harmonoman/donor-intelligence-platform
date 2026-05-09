"""
Unit tests for pipeline run logging utility.
Ticket 6.1: pipeline_run_log Metadata Table

Tests validate:
- log_run() inserts one row per call
- Two calls append two rows, no overwrite
- Required fields are present and correct
- Status must be PASS or FAIL only
- Timestamp is populated automatically

Run with:
    uv run pytest tests/unit/test_log_run.py -v
"""


import pytest
from google.cloud import bigquery

from pipelines.utils.env import get_required_env, load_env
from pipelines.utils.log_run import LOG_TABLE, log_run, validate_status

# ---------------------------------------------------------------------------
# Status validation tests (pure logic, no BigQuery)
# ---------------------------------------------------------------------------

def test_validate_status_pass():
    """PASS is a valid status."""
    assert validate_status("PASS") == "PASS"


def test_validate_status_fail():
    """FAIL is a valid status."""
    assert validate_status("FAIL") == "FAIL"


def test_validate_status_invalid():
    """Any value other than PASS or FAIL raises ValueError."""
    with pytest.raises(ValueError):
        validate_status("SUCCESS")


def test_validate_status_lowercase_raises():
    """Lowercase pass or fail raises ValueError."""
    with pytest.raises(ValueError):
        validate_status("pass")
    with pytest.raises(ValueError):
        validate_status("fail")


def test_validate_status_empty_raises():
    """Empty string raises ValueError."""
    with pytest.raises(ValueError):
        validate_status("")


# ---------------------------------------------------------------------------
# log_run() integration tests (real BigQuery)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bq_client():
    load_env()
    project_id = get_required_env("GCP_PROJECT_ID")
    return bigquery.Client(project=project_id)


@pytest.fixture(scope="module")
def project_id():
    load_env()
    return get_required_env("GCP_PROJECT_ID")


def test_log_run_inserts_one_row(bq_client, project_id):
    """log_run() inserts exactly one row into pipeline_run_log."""

    run_id = "test-single-row-001"

    # Count rows before
    before = list(bq_client.query(
        f"SELECT COUNT(*) as cnt FROM `{project_id}.{LOG_TABLE}` WHERE run_id = '{run_id}'"
    ).result())[0]["cnt"]

    log_run(
        run_id=run_id,
        execution_date="2099-01-01",
        task_name="test_task",
        row_count_input=100,
        row_count_output=95,
        status="PASS",
    )

    after = list(bq_client.query(
        f"SELECT COUNT(*) as cnt FROM `{project_id}.{LOG_TABLE}` WHERE run_id = '{run_id}'"
    ).result())[0]["cnt"]

    assert after == before + 1, "log_run() should insert exactly one row"


def test_log_run_appends_not_overwrites(bq_client, project_id):
    """Calling log_run() twice results in two rows, not one."""
    run_id = "test-append-002"

    # Count rows before to handle reruns
    before = list(bq_client.query(
        f"SELECT COUNT(*) as cnt FROM `{project_id}.{LOG_TABLE}` WHERE run_id = '{run_id}'"
    ).result())[0]["cnt"]

    log_run(
        run_id=run_id,
        execution_date="2099-01-01",
        task_name="test_task_append",
        row_count_input=200,
        row_count_output=198,
        status="PASS",
    )
    log_run(
        run_id=run_id,
        execution_date="2099-01-01",
        task_name="test_task_append",
        row_count_input=200,
        row_count_output=198,
        status="PASS",
    )

    after = list(bq_client.query(
        f"SELECT COUNT(*) as cnt FROM `{project_id}.{LOG_TABLE}` WHERE run_id = '{run_id}'"
    ).result())[0]["cnt"]

    assert after == before + 2, "log_run() should append exactly 2 rows"


def test_log_run_fields_are_correct(bq_client, project_id):
    """Fields written to BigQuery match the inputs exactly."""

    run_id = "test-fields-003"

    log_run(
        run_id=run_id,
        execution_date="2099-06-15",
        task_name="test_field_check",
        row_count_input=500,
        row_count_output=499,
        status="FAIL",
    )

    rows = list(bq_client.query(
        f"""
        SELECT *
        FROM `{project_id}.{LOG_TABLE}`
        WHERE run_id = '{run_id}'
        ORDER BY timestamp DESC
        LIMIT 1
        """
    ).result())

    assert len(rows) == 1
    row = rows[0]
    assert row["run_id"] == run_id
    assert str(row["execution_date"]) == "2099-06-15"
    assert row["task_name"] == "test_field_check"
    assert row["row_count_input"] == 500
    assert row["row_count_output"] == 499
    assert row["status"] == "FAIL"
    assert row["timestamp"] is not None


def test_log_run_timestamp_is_populated(bq_client, project_id):
    """Timestamp field is automatically populated on insert."""
    from pipelines.utils.log_run import LOG_TABLE

    run_id = "test-timestamp-004"

    log_run(
        run_id=run_id,
        execution_date="2099-01-01",
        task_name="test_timestamp",
        row_count_input=10,
        row_count_output=10,
        status="PASS",
    )

    rows = list(bq_client.query(
        f"""
        SELECT timestamp
        FROM `{project_id}.{LOG_TABLE}`
        WHERE run_id = '{run_id}'
        LIMIT 1
        """
    ).result())

    assert len(rows) == 1
    assert rows[0]["timestamp"] is not None


def test_log_run_handles_none_row_counts(bq_client, project_id):
    """None row counts should not raise errors and default to 0."""
    run_id = "test-none-counts-005"
    log_run(
        run_id=run_id,
        execution_date="2099-01-01",
        task_name="test_none_counts",
        row_count_input=None,
        row_count_output=None,
        status="PASS",
    )
    rows = list(bq_client.query(
        f"""
        SELECT row_count_input, row_count_output
        FROM `{project_id}.{LOG_TABLE}`
        WHERE run_id = '{run_id}'
        LIMIT 1
        """
    ).result())
    assert len(rows) == 1
    assert rows[0]["row_count_input"] == 0
    assert rows[0]["row_count_output"] == 0
