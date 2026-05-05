"""
Integration tests for identity resolution.
Tickets 4.2 + 4.3 — dim_donors Core Matching Logic

Tests validate fixture dataset outcomes BEFORE real data is processed.
All fixture scenarios must pass before running against full FEC sample.

Run with:
    uv run pytest tests/integration/test_build_identity.py -v
"""

import csv
from datetime import date
from pathlib import Path

import pytest
from google.cloud import bigquery

from pipelines.identity.build_identity import (
    load_fixture_to_staging_temp,
    run_identity_resolution,
)
from pipelines.utils.env import get_required_env

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIXTURE_PATH = Path("tests/fixtures/identity_fixtures.csv")
TEST_EXECUTION_DATE = date(2099, 6, 1)  # far future — won't collide with real data
TEMP_STAGING_TABLE = "staging.stg_identity_test"
TEST_DIM_DONORS_TABLE = "core.dim_donors_test"
TEST_UNRESOLVED_TABLE = "core.dim_donors_unresolved_test" # reserved — not populated until post-MVP


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
def identity_results(bq_client, project_id):
    """
    Load fixture into temp staging table, run identity resolution,
    return results as a dict keyed by record_id for easy assertion.
    """
    load_fixture_to_staging_temp(
        bq_client, project_id, FIXTURE_PATH,
        TEST_EXECUTION_DATE, TEMP_STAGING_TABLE
    )
    run_identity_resolution(
        bq_client, project_id,
        source_table=TEMP_STAGING_TABLE,
        dim_donors_table=TEST_DIM_DONORS_TABLE,
        unresolved_table=TEST_UNRESOLVED_TABLE, # reserved — not populated until post-MVP
        execution_date=TEST_EXECUTION_DATE,
    )
    # Load results indexed by sub_id for easy lookup
    query = f"""
        SELECT sub_id, donor_id, match_rule, identity_conflict
        FROM `{project_id}.{TEST_DIM_DONORS_TABLE}`
        WHERE _load_date = DATE('{TEST_EXECUTION_DATE.isoformat()}')
    """
    rows = list(bq_client.query(query).result())
    return {row["sub_id"]: dict(row) for row in rows}


# ---------------------------------------------------------------------------
# Scenario 1 — Rule 1 Exact Match (Name + ZIP)
# ---------------------------------------------------------------------------

def test_rule1_match_smith_john(identity_results):
    """R001 and R002 share name+ZIP — must resolve to same donor_id."""
    r001 = identity_results.get("R001")
    r002 = identity_results.get("R002")
    assert r001 is not None, "R001 not found in dim_donors"
    assert r002 is not None, "R002 not found in dim_donors"
    assert r001["donor_id"] == r002["donor_id"], (
        f"R001 and R002 should share donor_id — got {r001['donor_id']} and {r002['donor_id']}"
    )
    assert r001["match_rule"] in ("rule1", "no_match")
    assert r001["identity_conflict"] is False


def test_rule1_match_jones_mary(identity_results):
    """R003 and R004 share name+ZIP — must resolve to same donor_id."""
    r003 = identity_results.get("R003")
    r004 = identity_results.get("R004")
    assert r003 is not None
    assert r004 is not None
    assert r003["donor_id"] == r004["donor_id"]
    assert r003["identity_conflict"] is False


def test_rule1_match_harris_janet_punctuation(identity_results):
    """R021 and R022 differ only by trailing period — must normalize to same donor_id."""
    r021 = identity_results.get("R021")
    r022 = identity_results.get("R022")
    assert r021 is not None
    assert r022 is not None
    assert r021["donor_id"] == r022["donor_id"], (
        "Punctuation difference should not prevent Rule 1 match"
    )


# ---------------------------------------------------------------------------
# Scenario 2 — Rule 2 Exact Match (Name + Address)
# ---------------------------------------------------------------------------

def test_rule2_match_obrien_patrick(identity_results):
    """R005 and R006 match on name+address — R006 has no ZIP."""
    r005 = identity_results.get("R005")
    r006 = identity_results.get("R006")
    assert r005 is not None
    assert r006 is not None
    assert r005["donor_id"] == r006["donor_id"], (
        "R005 and R006 should match via Rule 2 — same name and address"
    )
    assert r006["match_rule"] == "rule2"
    assert r006["identity_conflict"] is False


# ---------------------------------------------------------------------------
# Scenario 3 — No Match (New Donor)
# ---------------------------------------------------------------------------

def test_no_match_records_get_unique_donor_ids(identity_results):
    """No-match records must each receive a unique donor_id."""
    no_match_records = ["R007", "R008", "R009", "R010", "R011", "R012", "R020", "R023"]
    donor_ids = []
    for record_id in no_match_records:
        row = identity_results.get(record_id)
        assert row is not None, f"{record_id} not found in dim_donors"
        assert row["identity_conflict"] is False
        donor_ids.append(row["donor_id"])
    assert len(set(donor_ids)) == len(donor_ids), (
        "No-match records should each have a unique donor_id"
    )


def test_name_suffix_prevents_match(identity_results):
    """R007 (davis carol ms) and R008 (davis carol) must NOT share donor_id."""
    r007 = identity_results.get("R007")
    r008 = identity_results.get("R008")
    assert r007 is not None
    assert r008 is not None
    assert r007["donor_id"] != r008["donor_id"], (
        "Name suffix difference should prevent matching"
    )


def test_null_address_record_gets_new_donor_id(identity_results):
    """R023 has no ZIP and no address — must get a new donor_id."""
    r023 = identity_results.get("R023")
    assert r023 is not None, "R023 not found in dim_donors"
    assert r023["identity_conflict"] is False
    assert r023["match_rule"] == "no_match"


# ---------------------------------------------------------------------------
# Scenario 4 — Same Name + ZIP (Resolved as Same Donor)
# ---------------------------------------------------------------------------

def test_same_name_zip_resolves_to_same_donor(identity_results):
    """R013 and R014 share name+ZIP — resolved to same donor_id."""
    r013 = identity_results.get("R013")
    r014 = identity_results.get("R014")
    assert r013 is not None
    assert r014 is not None
    assert r013["donor_id"] == r014["donor_id"], (
        "R013 and R014 share name+ZIP — should resolve to same donor_id"
    )
    assert r013["identity_conflict"] is False


def test_same_name_zip_resolves_to_same_donor_lee(identity_results):
    """R015 and R016 share name+ZIP — resolved to same donor_id."""
    r015 = identity_results.get("R015")
    r016 = identity_results.get("R016")
    assert r015 is not None
    assert r016 is not None
    assert r015["donor_id"] == r016["donor_id"]
    assert r015["identity_conflict"] is False


# ---------------------------------------------------------------------------
# Scenario 5 — Same Name, Different ZIP (Separate Donors)
# ---------------------------------------------------------------------------

def test_different_zip_gets_different_donor_id(identity_results):
    """R017/R018 share ZIP 37201, R019 has ZIP 37202 — separate donor_ids."""
    r017 = identity_results.get("R017")
    r018 = identity_results.get("R018")
    r019 = identity_results.get("R019")
    assert r017 is not None
    assert r018 is not None
    assert r019 is not None
    assert r017["donor_id"] == r018["donor_id"], (
        "R017 and R018 share name+ZIP — should resolve to same donor_id"
    )
    assert r019["donor_id"] != r017["donor_id"], (
        "R019 has different ZIP — should get a different donor_id"
    )
    assert r019["identity_conflict"] is False


# ---------------------------------------------------------------------------
# Scenario 6 — Rule 2 Multi-Match MIN Behavior
# ---------------------------------------------------------------------------

def test_rule2_multi_zip_min_behavior(identity_results):
    """R026 has no ZIP — Rule 2 finds R024 and R025, MIN picks R024's key."""
    r024 = identity_results.get("R024")
    r025 = identity_results.get("R025")
    r026 = identity_results.get("R026")
    assert r024 is not None
    assert r025 is not None
    assert r026 is not None
    # R024 and R026 should share a donor_id (MIN picks R024's canonical key)
    assert r024["donor_id"] == r026["donor_id"], (
        "R026 should resolve to R024's donor_id via Rule 2 MIN"
    )
    # R025 should have a different donor_id (different ZIP)
    assert r025["donor_id"] != r024["donor_id"], (
        "R025 has a different ZIP — should have a different donor_id"
    )
    assert r026["match_rule"] == "rule2"

# ---------------------------------------------------------------------------
# General integrity checks
# ---------------------------------------------------------------------------

def test_all_fixture_records_in_dim_donors(identity_results):
    """Every fixture record must appear in dim_donors."""
    with open(FIXTURE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        record_ids = [row["record_id"] for row in reader]

    for record_id in record_ids:
        assert record_id in identity_results, (
            f"{record_id} not found in dim_donors"
        )


def test_non_conflict_donor_ids_are_unique(identity_results):
    """Non-conflict records that are not expected matches must have unique donor_ids."""
    # Collect donor_ids for records that should be unique
    unique_record_groups = [
        ["R009"], ["R010"], ["R011"], ["R012"], ["R020"], ["R023"]
    ]
    donor_ids = []
    for group in unique_record_groups:
        donor_ids.append(identity_results[group[0]]["donor_id"])
    assert len(set(donor_ids)) == len(donor_ids), (
        "Independent no-match records should have unique donor_ids"
    )
