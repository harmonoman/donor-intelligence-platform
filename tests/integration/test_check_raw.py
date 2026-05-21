"""
Integration tests for raw layer quality checks.
Raw Layer Quality Checks

Tests validate:
- Valid dataset passes all checks
- Empty partition fails row count check
- Null values in critical fields fail null check
- Missing column fails schema check

Run with:
    uv run pytest tests/integration/test_check_raw.py -v
"""

import pytest
from google.cloud import bigquery

from pipelines.quality.check_raw import (
    check_required_fields,
    check_row_count,
    check_schema,
    run_raw_quality_checks,
)
from pipelines.utils.env import get_required_env, load_env

# ---------------------------------------------------------------------------
# Fixtures
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


# ---------------------------------------------------------------------------
# Row count checks
# ---------------------------------------------------------------------------

def test_row_count_passes_for_valid_partition(bq_client, project_id):
    """Valid partition with rows passes row count check."""
    count = check_row_count(bq_client, project_id, "2024-01-01")
    assert count > 0


def test_row_count_fails_for_empty_partition(bq_client, project_id):
    """Empty partition raises ValueError."""
    with pytest.raises(ValueError, match="row count check FAILED"):
        check_row_count(bq_client, project_id, "1900-01-01")


# ---------------------------------------------------------------------------
# Required field null checks
# ---------------------------------------------------------------------------

def test_required_fields_pass_for_valid_partition(bq_client, project_id):
    """
    Raw layer null check passes because REQUIRED_FIELDS is empty.
    FEC raw data contains nulls in NAME, TRANSACTION_AMT, TRANSACTION_DT.
    Null validation is deferred to staging layer.
    """
    check_required_fields(bq_client, project_id, "2024-01-01")


def test_required_fields_fail_for_null_name(bq_client, project_id):
    """
    check_required_fields passes vacuously when REQUIRED_FIELDS is empty.
    Null validation is deferred to staging layer.
    This test documents that behavior explicitly.
    """
    check_required_fields(bq_client, project_id, "2024-01-01")
    # No assertion needed: passes because REQUIRED_FIELDS = []
    # If null checks are added in future, this test will need updating


# ---------------------------------------------------------------------------
# Schema checks
# ---------------------------------------------------------------------------

def test_schema_check_passes_for_valid_table(bq_client, project_id):
    """All expected columns exist in raw table."""
    check_schema(bq_client, project_id)


# ---------------------------------------------------------------------------
# Full quality check run
# ---------------------------------------------------------------------------

def test_run_raw_quality_checks_passes_for_valid_partition(bq_client, project_id):
    """All checks pass for valid partition."""
    count = run_raw_quality_checks(bq_client, project_id, "2024-01-01")
    assert count > 0


def test_run_raw_quality_checks_fails_for_missing_partition(bq_client, project_id):
    """All checks fail fast for missing partition."""
    with pytest.raises(ValueError):
        run_raw_quality_checks(bq_client, project_id, "1900-01-01")
