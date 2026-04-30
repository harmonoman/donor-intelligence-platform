"""
Integration test — BigQuery connectivity and dataset existence.

Ticket 1.2 — GCP Project + BigQuery Environment Setup

Run after setup_bigquery.py has been executed:
    uv run pytest tests/integration/test_bigquery_connection.py -v
"""

import pytest
from google.cloud import bigquery
from pipelines.utils.env import get_required_env


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bq_client():
    """Authenticated BigQuery client via Application Default Credentials."""
    project_id = get_required_env("GCP_PROJECT_ID")
    return bigquery.Client(project=project_id)


@pytest.fixture(scope="module")
def project_id():
    return get_required_env("GCP_PROJECT_ID")


# ---------------------------------------------------------------------------
# Tests — write these first, they will fail until setup_bigquery.py runs
# ---------------------------------------------------------------------------

REQUIRED_DATASETS = ["raw", "staging", "core", "marts", "metadata"]


def test_bigquery_client_connects(bq_client):
    """Client can be instantiated and reach BigQuery."""
    assert bq_client is not None
    assert bq_client.project is not None


def test_bigquery_query_executes(bq_client):
    """A simple query executes and returns a result."""
    query = "SELECT 1 AS health_check"
    result = list(bq_client.query(query).result())
    assert len(result) == 1
    assert result[0]["health_check"] == 1


def test_required_datasets_exist(bq_client, project_id):
    """All five pipeline datasets exist in BigQuery."""
    existing = {ds.dataset_id for ds in bq_client.list_datasets()}
    missing = [ds for ds in REQUIRED_DATASETS if ds not in existing]
    assert not missing, (
        f"Missing datasets: {missing}\n"
        f"Run: uv run python scripts/setup_bigquery.py"
    )


def test_dataset_locations(bq_client, project_id):
    """All datasets are in the expected region."""
    for dataset_id in REQUIRED_DATASETS:
        ref = bq_client.get_dataset(f"{project_id}.{dataset_id}")
        assert ref.location is not None, (
            f"Dataset {dataset_id} has no location set"
        )
