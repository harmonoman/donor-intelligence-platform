"""
BigQuery Environment Setup Script
Ticket 1.2 — GCP Project + BigQuery Environment Setup

Purpose:
    Create all required BigQuery datasets for the Donor Intelligence Platform.
    Idempotent — safe to run multiple times.

Usage:
    uv run python scripts/setup_bigquery.py
"""

import os
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import Conflict


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REQUIRED_DATASETS = [
    "raw",
    "staging",
    "core",
    "marts",
    "metadata",
]

DATASET_LOCATION = "US"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_required_env(var: str) -> str:
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{var}' is not set.\n"
            f"Copy .env.example to .env and populate it."
        )
    return value


def load_env():
    """Load .env file if present. Fail clearly if not."""
    env_path = Path(".env")
    if not env_path.exists():
        raise FileNotFoundError(
            ".env file not found.\n"
            "Copy .env.example to .env and populate GCP_PROJECT_ID "
            "and GOOGLE_APPLICATION_CREDENTIALS."
        )
    # Load manually — no python-dotenv dependency
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def setup():
    load_env()

    project_id = get_required_env("GCP_PROJECT_ID")

    print(f"\n{'='*60}")
    print("BIGQUERY ENVIRONMENT SETUP")
    print(f"{'='*60}")
    print(f"Project  : {project_id}")
    print(f"Location : {DATASET_LOCATION}")
    print(f"{'='*60}\n")

    client = bigquery.Client(project=project_id)

    for dataset_id in REQUIRED_DATASETS:
        full_id = f"{project_id}.{dataset_id}"
        dataset = bigquery.Dataset(full_id)
        dataset.location = DATASET_LOCATION

        try:
            client.create_dataset(dataset, timeout=30)
            print(f"✅ Created  : {full_id}")
        except Conflict:
            print(f"✓  Exists   : {full_id}")

    # Validate connection with a test query
    print("\n--- Validating Connection ---")
    result = list(client.query("SELECT 1 AS health_check").result())
    assert result[0]["health_check"] == 1
    print("✅ BigQuery connection verified")

    # List created datasets
    print("\n--- Datasets in Project ---")
    for ds in client.list_datasets():
        print(f"  {project_id}.{ds.dataset_id}")

    print(f"\n{'='*60}")
    print("SETUP COMPLETE")
    print(f"{'='*60}")
    print("Next: uv run pytest tests/integration/test_bigquery_connection.py -v")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    setup()
