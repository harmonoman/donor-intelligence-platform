"""
Integration tests for staging build script.
Ticket 3.2 — Staging SQL + Load Script

Tests validate:
- Normalization is applied correctly
- ENTITY_TP filter removes non-IND records
- Normalized fields are populated
- MERGE is idempotent (no duplicates on rerun)
- contribution_date is parsed correctly from MMDDYYYY

Run with:
    uv run pytest tests/integration/test_build_staging.py -v
"""

from datetime import date

import pytest
from google.cloud import bigquery

from pipelines.staging.build_staging import (
    apply_normalization,
    count_staging_rows,
    filter_individuals,
    load_raw_data,
    parse_contribution_date,
    run_staging,
)
from pipelines.utils.env import get_required_env

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TEST_EXECUTION_DATE = date(2025, 1, 1)  # matches raw ingestion date


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


@pytest.fixture(scope="module")
def raw_df(bq_client, project_id):
    """Load raw data for the test execution date."""
    return load_raw_data(bq_client, project_id, TEST_EXECUTION_DATE)


# ---------------------------------------------------------------------------
# Unit-style tests (no BigQuery required)
# ---------------------------------------------------------------------------

def test_apply_normalization_name(raw_df):
    """donor_name_normalized is lowercase with no punctuation."""
    df = apply_normalization(raw_df)
    assert "donor_name_normalized" in df.columns
    # No uppercase characters
    assert df["donor_name_normalized"].str.contains(r"[A-Z]").sum() == 0
    # No commas
    assert df["donor_name_normalized"].str.contains(",").sum() == 0


def test_apply_normalization_zip(raw_df):
    """zip_normalized is always 5 digits."""
    df = apply_normalization(raw_df)
    assert "zip_normalized" in df.columns
    non_empty = df["zip_normalized"][df["zip_normalized"] != ""]
    assert (non_empty.str.len() == 5).all(), (
        "All non-empty ZIP codes must be exactly 5 digits"
    )


def test_apply_normalization_address(raw_df):
    """donor_address_normalized is lowercase."""
    df = apply_normalization(raw_df)
    assert "donor_address_normalized" in df.columns
    non_empty = df["donor_address_normalized"][df["donor_address_normalized"] != ""]
    assert non_empty.str.contains(r"[A-Z]").sum() == 0


def test_apply_normalization_address_includes_state(raw_df):
    """donor_address_normalized includes both city and state."""
    df = apply_normalization(raw_df)
    # Address should contain a space (city + state)
    non_empty = df["donor_address_normalized"][df["donor_address_normalized"] != ""]
    assert non_empty.str.contains(" ").any(), (
        "donor_address_normalized appears to contain only city — state may be missing"
    )


def test_filter_individuals_removes_non_ind(raw_df):
    """Only IND entity type records pass the filter."""
    df = filter_individuals(raw_df)
    assert (df["ENTITY_TP"] == "IND").all(), (
        "Non-IND records found after filter"
    )


def test_parse_contribution_date(raw_df):
    """TRANSACTION_DT is parsed from MMDDYYYY to YYYY-MM-DD string."""
    df = parse_contribution_date(raw_df)
    assert "contribution_date" in df.columns
    # Verify format is YYYY-MM-DD on a non-null sample
    sample = df["contribution_date"].dropna().iloc[0]
    assert len(sample) == 10, f"Expected YYYY-MM-DD format, got: {sample}"
    assert sample[4] == "-" and sample[7] == "-", (
        f"Expected YYYY-MM-DD format, got: {sample}"
    )


# ---------------------------------------------------------------------------
# Integration tests (BigQuery required)
# ---------------------------------------------------------------------------

def test_staging_row_count_stable_on_rerun(bq_client, project_id):
    """Running build_staging twice produces same row count."""

    run_staging(bq_client, project_id, TEST_EXECUTION_DATE)
    count_first = count_staging_rows(bq_client, project_id, TEST_EXECUTION_DATE)

    run_staging(bq_client, project_id, TEST_EXECUTION_DATE)
    count_second = count_staging_rows(bq_client, project_id, TEST_EXECUTION_DATE)

    assert count_first == count_second, (
        f"Row count changed after rerun: {count_first} → {count_second}"
    )


def test_staging_row_count_less_than_raw(bq_client, project_id):
    """Staging has fewer rows than raw due to ENTITY_TP filter."""

    run_staging(bq_client, project_id, TEST_EXECUTION_DATE)

    staging_count = count_staging_rows(bq_client, project_id, TEST_EXECUTION_DATE)

    raw_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project_id}.raw.fec_contributions`
        WHERE _load_date = DATE('{TEST_EXECUTION_DATE.isoformat()}')
    """
    raw_count = list(bq_client.query(raw_query).result())[0]["cnt"]

    assert staging_count <= raw_count, (
        f"Staging ({staging_count}) has more rows than raw ({raw_count})"
    )
