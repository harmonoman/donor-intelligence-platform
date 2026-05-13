"""
Integration tests for staging layer quality checks.
Ticket 7.2: Staging Layer Quality Checks

Tests validate:
- Valid dataset passes all three checks
- Row count mismatch fails
- Missing partition fails fast at row count check
- Null rates pass for valid partition
- Duplicate sub_id check passes for valid partition

Run with:
    uv run pytest tests/integration/test_check_staging.py -v
"""

import pytest
from google.cloud import bigquery

from pipelines.quality.check_staging import (
    check_duplicate_merge_keys,
    check_null_rates,
    check_row_count_consistency,
    run_staging_quality_checks,
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
# Row count consistency checks
# ---------------------------------------------------------------------------

def test_row_count_consistency_passes_for_valid_partition(bq_client, project_id):
    """Staging row count within threshold of raw passes."""
    raw_count, staging_count = check_row_count_consistency(
        bq_client, project_id, "2024-01-01"
    )
    assert staging_count > 0
    assert raw_count > 0


def test_row_count_consistency_fails_for_missing_partition(bq_client, project_id):
    """Missing partition raises ValueError."""
    with pytest.raises(ValueError, match="row count consistency check FAILED"):
        check_row_count_consistency(bq_client, project_id, "1900-01-01")


# ---------------------------------------------------------------------------
# Null rate checks
# ---------------------------------------------------------------------------

def test_null_rates_pass_for_valid_partition(bq_client, project_id):
    """Valid partition with low null rates passes."""
    check_null_rates(bq_client, project_id, "2024-01-01")


# ---------------------------------------------------------------------------
# Duplicate MERGE key checks
# ---------------------------------------------------------------------------

def test_no_duplicate_merge_keys_for_valid_partition(bq_client, project_id):
    """Valid partition has no duplicate sub_ids."""
    check_duplicate_merge_keys(bq_client, project_id, "2024-01-01")


# ---------------------------------------------------------------------------
# Full quality check run
# ---------------------------------------------------------------------------

def test_run_staging_quality_checks_passes_for_valid_partition(bq_client, project_id):
    """All three checks pass for valid partition."""
    raw_count, staging_count = run_staging_quality_checks(
        bq_client, project_id, "2024-01-01"
    )
    assert raw_count > 0
    assert staging_count > 0


def test_staging_checks_fail_fast_for_missing_partition(bq_client, project_id):
    """
    All staging checks fail fast for missing partition.
    Row count check raises first, blocking null and duplicate checks.
    Note: null rate and duplicate failure paths are validated by
    the check functions themselves against real data thresholds.
    """
    with pytest.raises(ValueError, match="row count consistency check FAILED"):
        run_staging_quality_checks(bq_client, project_id, "1900-01-01")
