"""
Integration tests for mart layer quality checks.
Mart Layer Quality Checks

Tests validate:
- Valid dataset passes all three checks
- Grain check: one row per donor_id
- Null engagement score check: zero nulls allowed
- Row count consistency: mart == eligible dim_donors donors
- Duplicate donor_id raises ValueError (mock)
- Null engagement score raises ValueError (mock)
- Row count mismatch raises ValueError (mock)
- Empty mart raises ValueError (mock)
...
"""

from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from pipelines.quality.check_mart import (
    check_grain,
    check_null_engagement_scores,
    check_row_count_consistency,
    run_mart_quality_checks,
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
# Grain checks
# ---------------------------------------------------------------------------

def test_grain_passes_for_valid_mart(bq_client, project_id):
    """One row per donor_id in mart_donor_summary."""
    count, distinct_count = check_grain(bq_client, project_id)
    assert count == distinct_count
    assert count > 0


# ---------------------------------------------------------------------------
# Null engagement score checks
# ---------------------------------------------------------------------------

def test_null_engagement_scores_pass_for_valid_mart(bq_client, project_id):
    """No null engagement scores in mart_donor_summary."""
    check_null_engagement_scores(bq_client, project_id)


# ---------------------------------------------------------------------------
# Row count consistency checks
# ---------------------------------------------------------------------------

def test_row_count_consistency_passes_for_valid_mart(bq_client, project_id):
    """Mart row count matches eligible dim_donors count."""
    mart_count, eligible_count = check_row_count_consistency(bq_client, project_id)
    assert mart_count == eligible_count
    assert mart_count > 0


# ---------------------------------------------------------------------------
# Full quality check run
# ---------------------------------------------------------------------------

def test_run_mart_quality_checks_passes(bq_client, project_id):
    """All three checks pass for valid mart."""
    mart_count = run_mart_quality_checks(bq_client, project_id)
    assert mart_count > 0


def test_check_grain_fails_for_duplicates():
    """check_grain raises ValueError when duplicate donor_ids exist."""
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = [
        {"total_count": 100, "distinct_count": 98}
    ]
    with pytest.raises(ValueError, match="check_mart grain check FAILED"):
        check_grain(mock_client, "test-project")


def test_check_grain_fails_for_empty_mart():
    """check_grain raises ValueError when mart is empty."""
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = [
        {"total_count": 0, "distinct_count": 0}
    ]
    with pytest.raises(ValueError, match="mart_donor_summary is empty"):
        check_grain(mock_client, "test-project")


def test_check_null_engagement_scores_fails_for_nulls():
    """check_null_engagement_scores raises ValueError when nulls exist."""
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = [
        {"total": 100, "null_count": 5}
    ]
    with pytest.raises(ValueError, match="check_mart null score check FAILED"):
        check_null_engagement_scores(mock_client, "test-project")


def test_check_row_count_consistency_fails_for_mismatch():
    """check_row_count_consistency raises ValueError when counts differ."""
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = [
        {"mart_count": 100, "eligible_count": 102}
    ]
    with pytest.raises(ValueError, match="check_mart row count check FAILED"):
        check_row_count_consistency(mock_client, "test-project")
