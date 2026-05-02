"""
Integration tests for raw FEC ingestion.
Ticket 2.2 — Raw Ingestion Script

Tests run against a small fixture CSV and a real BigQuery table.
BigQuery is required — these are integration tests, not unit tests.

Run with:
    uv run pytest tests/integration/test_load_raw_fec.py -v
"""

import csv
from datetime import date
from pathlib import Path

import pytest
from google.cloud import bigquery

from pipelines.ingest.load_raw_fec import (
    add_load_date_column,
    count_rows_in_partition,
    load_csv_to_dataframe,
    load_to_bigquery,
)
from pipelines.utils.env import get_required_env, load_env

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIXTURE_PATH = Path("tests/fixtures/fec_fixture.csv")
TEST_EXECUTION_DATE = date(2099, 1, 1)
TEST_TABLE_ID = "raw.fec_contributions_test"  # separate test table


# ---------------------------------------------------------------------------
# Teardown — clean up test partition after suite completes
# ---------------------------------------------------------------------------

def pytest_sessionfinish(session, exitstatus):
    """
    Delete the test partition from BigQuery after all tests complete.
    Prevents 2099-01-01 test data from accumulating across test runs.
    """
    try:
        load_env()
        project_id = get_required_env("GCP_PROJECT_ID")
        client = bigquery.Client(project=project_id)
        partition_id = TEST_EXECUTION_DATE.strftime("%Y%m%d")
        full_table_id = f"{project_id}.{TEST_TABLE_ID}${partition_id}"
        client.delete_table(full_table_id, not_found_ok=True)
        print(f"\n🧹 Cleaned up test partition: {full_table_id}")
    except Exception as e:
        print(f"\n⚠️  Could not clean up test partition: {e}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bq_client():
    project_id = get_required_env("GCP_PROJECT_ID")
    return bigquery.Client(project=project_id)


@pytest.fixture(scope="module")
def project_id():
    return get_required_env("GCP_PROJECT_ID")


# ---------------------------------------------------------------------------
# Unit-style tests (no BigQuery required)
# ---------------------------------------------------------------------------

def test_fixture_file_exists():
    """Fixture CSV must exist before ingestion tests run."""
    assert FIXTURE_PATH.exists(), (
        f"Fixture file not found at {FIXTURE_PATH}"
    )


def test_load_csv_to_dataframe():
    """CSV loads into a dataframe with correct column count."""
    df = load_csv_to_dataframe(FIXTURE_PATH)
    assert len(df) == 5, f"Expected 5 rows, got {len(df)}"
    assert "NAME" in df.columns
    assert "SUB_ID" in df.columns
    assert "TRANSACTION_AMT" in df.columns


def test_add_load_date_column():
    """_load_date column is added with correct value."""
    df = load_csv_to_dataframe(FIXTURE_PATH)
    df = add_load_date_column(df, TEST_EXECUTION_DATE)
    assert "_load_date" in df.columns
    assert df["_load_date"].iloc[0] == TEST_EXECUTION_DATE


def test_load_date_is_same_for_all_rows():
    """Every row gets the same _load_date."""
    df = load_csv_to_dataframe(FIXTURE_PATH)
    df = add_load_date_column(df, TEST_EXECUTION_DATE)
    assert df["_load_date"].nunique() == 1


# ---------------------------------------------------------------------------
# Integration tests (BigQuery required)
# ---------------------------------------------------------------------------

def test_load_to_bigquery_succeeds(bq_client, project_id):
    """Fixture CSV loads into BigQuery test table without error."""
    df = load_csv_to_dataframe(FIXTURE_PATH)
    df = add_load_date_column(df, TEST_EXECUTION_DATE)
    load_to_bigquery(df, project_id, TEST_TABLE_ID, TEST_EXECUTION_DATE, bq_client)

    count = count_rows_in_partition(
        bq_client, project_id, TEST_TABLE_ID, TEST_EXECUTION_DATE
    )
    assert count == 5, f"Expected 5 rows in partition, got {count}"


def test_row_count_matches_fixture(bq_client, project_id):
    """Row count in BigQuery matches source fixture file."""
    with open(FIXTURE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)  # skip header
        csv_count = sum(1 for _ in reader)

    bq_count = count_rows_in_partition(
        bq_client, project_id, TEST_TABLE_ID, TEST_EXECUTION_DATE
    )
    assert bq_count == csv_count, (
        f"BigQuery row count ({bq_count}) != CSV row count ({csv_count})"
    )


def test_idempotency_no_duplicates_on_rerun(bq_client, project_id):
    """Running the load twice for the same date does not duplicate rows."""
    df = load_csv_to_dataframe(FIXTURE_PATH)
    df = add_load_date_column(df, TEST_EXECUTION_DATE)

    # Run once
    load_to_bigquery(df, project_id, TEST_TABLE_ID, TEST_EXECUTION_DATE, bq_client)
    # Run again
    load_to_bigquery(df, project_id, TEST_TABLE_ID, TEST_EXECUTION_DATE, bq_client)

    count = count_rows_in_partition(
        bq_client, project_id, TEST_TABLE_ID, TEST_EXECUTION_DATE
    )
    assert count == 5, (
        f"Expected 5 rows after rerun, got {count} — idempotency failed"
    )
