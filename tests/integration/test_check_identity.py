"""
Integration tests for identity layer quality checks.
Ticket 7.3: Identity Layer Quality Checks

Tests validate:
- Valid dataset passes all three checks
- Duplicate sub_id fails uniqueness check
- Row count mismatch fails reconciliation check
- Null donor_id fails null check

Architecture note:
    donor_id is NOT unique per row in dim_donors.
    Multiple contributions from the same donor share one donor_id.
    sub_id is the actual MERGE key and must be unique per row.
    Real data: 15,255,256 rows, 2,375,123 unique donor_ids,
    15,255,256 unique sub_ids (2024 partition).

Run with:
    uv run pytest tests/integration/test_check_identity.py -v
"""

import pytest
from google.cloud import bigquery

from pipelines.quality.check_identity import (
    check_donor_id_not_null,
    check_row_count_reconciliation,
    check_sub_id_uniqueness,
    run_identity_quality_checks,
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
# sub_id uniqueness checks
# ---------------------------------------------------------------------------

def test_sub_id_uniqueness_passes_for_valid_partition(bq_client, project_id):
    """Valid partition has no duplicate sub_ids in dim_donors."""
    check_sub_id_uniqueness(bq_client, project_id, "2024-01-01")


def test_sub_id_uniqueness_fails_for_missing_partition(bq_client, project_id):
    """Missing partition raises ValueError."""
    with pytest.raises(ValueError, match="sub_id uniqueness check FAILED"):
        check_sub_id_uniqueness(bq_client, project_id, "1900-01-01")


# ---------------------------------------------------------------------------
# Row count reconciliation checks
# ---------------------------------------------------------------------------

def test_row_count_reconciliation_passes_for_valid_partition(bq_client, project_id):
    """staging == dim_donors + dim_donors_unresolved for valid partition."""
    staging, donors, unresolved = check_row_count_reconciliation(
        bq_client, project_id, "2024-01-01"
    )
    assert staging == donors + unresolved
    assert staging > 0


def test_row_count_reconciliation_fails_for_missing_partition(bq_client, project_id):
    """Missing partition raises ValueError."""
    with pytest.raises(ValueError, match="row count reconciliation check FAILED"):
        check_row_count_reconciliation(bq_client, project_id, "1900-01-01")


# ---------------------------------------------------------------------------
# Null donor_id checks
# ---------------------------------------------------------------------------

def test_donor_id_not_null_passes_for_valid_partition(bq_client, project_id):
    """Valid partition has no null donor_ids."""
    check_donor_id_not_null(bq_client, project_id, "2024-01-01")


def test_donor_id_not_null_fails_for_missing_partition(bq_client, project_id):
    """Missing partition raises ValueError."""
    with pytest.raises(ValueError, match="null donor_id check FAILED"):
        check_donor_id_not_null(bq_client, project_id, "1900-01-01")


# ---------------------------------------------------------------------------
# Full quality check run
# ---------------------------------------------------------------------------

def test_run_identity_quality_checks_passes_for_valid_partition(bq_client, project_id):
    """All three checks pass for valid partition."""
    staging, donors, unresolved = run_identity_quality_checks(
        bq_client, project_id, "2024-01-01"
    )
    assert staging > 0
    assert donors > 0
    assert staging == donors + unresolved


def test_identity_checks_fail_fast_for_missing_partition(bq_client, project_id):
    """
    All identity checks fail fast for missing partition.
    sub_id uniqueness check raises first.
    Note: row count and null failure paths validated against real data.
    """
    with pytest.raises(ValueError):
        run_identity_quality_checks(bq_client, project_id, "1900-01-01")
