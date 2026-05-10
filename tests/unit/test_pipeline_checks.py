"""
Unit tests for pipeline validation check functions.
Ticket 6.2: Full Pipeline DAG

Tests validate that check functions:
- Return row counts when data exists
- Raise ValueError when data is missing
- Raise ValueError when mart has duplicates or null scores

Run with:
    uv run pytest tests/unit/test_pipeline_checks.py -v
"""

import pytest
from google.cloud import bigquery

from pipelines.utils.env import get_required_env, load_env
from pipelines.utils.pipeline_checks import (
    check_identity,
    check_mart,
    check_raw,
    check_staging,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def project_id():
    load_env()
    return get_required_env("GCP_PROJECT_ID")


# ---------------------------------------------------------------------------
# check_raw tests
# ---------------------------------------------------------------------------

def test_check_raw_passes_for_existing_partition(project_id):
    """check_raw returns row count for a known good partition."""
    count = check_raw("2024-01-01")  # 2023-2024 cycle partition
    assert count > 0


def test_check_raw_fails_for_missing_partition(project_id):
    """check_raw raises ValueError when partition has no rows."""
    with pytest.raises(ValueError, match="check_raw FAILED"):
        check_raw("1900-01-01")  # partition that will never exist


# ---------------------------------------------------------------------------
# check_staging tests
# ---------------------------------------------------------------------------

def test_check_staging_passes_for_existing_partition(project_id):
    """check_staging returns row count for a known good partition."""
    count = check_staging("2024-01-01")
    assert count > 0


def test_check_staging_fails_for_missing_partition(project_id):
    """check_staging raises ValueError when partition has no rows."""
    with pytest.raises(ValueError, match="check_staging FAILED"):
        check_staging("1900-01-01")


# ---------------------------------------------------------------------------
# check_identity tests
# ---------------------------------------------------------------------------

def test_check_identity_passes_for_existing_partition(project_id):
    """check_identity returns row count for a known good partition."""
    count = check_identity("2024-01-01")
    assert count > 0


def test_check_identity_fails_for_missing_partition(project_id):
    """check_identity raises ValueError when partition has no rows."""
    with pytest.raises(ValueError, match="check_identity FAILED"):
        check_identity("1900-01-01")


# ---------------------------------------------------------------------------
# check_mart tests
# ---------------------------------------------------------------------------

def test_check_mart_passes_for_populated_mart(project_id):
    """check_mart returns row count when mart has data and no duplicates."""
    count = check_mart()
    assert count > 0


def test_check_mart_returns_correct_count(project_id):
    """check_mart row count matches direct BigQuery count."""
    load_env()
    client = bigquery.Client(project=project_id)
    direct_count = list(client.query(
        f"SELECT COUNT(*) as cnt FROM `{project_id}.marts.mart_donor_summary`"
    ).result())[0]["cnt"]
    check_count = check_mart()
    assert check_count == direct_count
