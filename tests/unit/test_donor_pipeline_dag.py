"""
Unit tests for donor_pipeline DAG structure.
Ticket 6.2: Full Pipeline DAG

Tests validate:
- DAG loads without errors
- All 8 tasks exist
- Task dependencies enforce correct order
- Fail-fast ordering is correct

Run with:
    uv run pytest tests/unit/test_donor_pipeline_dag.py -v
"""

import pytest
from airflow.models import DagBag

# ---------------------------------------------------------------------------
# DAG loading
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag("donor_pipeline")


# ---------------------------------------------------------------------------
# DAG existence tests
# ---------------------------------------------------------------------------

def test_dag_loads_without_errors(dagbag):
    """DAG file must load without import errors."""
    assert len(dagbag.import_errors) == 0, (
        f"DAG import errors: {dagbag.import_errors}"
    )


def test_dag_exists(dag):
    """donor_pipeline DAG must exist in DagBag."""
    assert dag is not None, "donor_pipeline DAG not found"


def test_dag_has_eight_tasks(dag):
    """DAG must have exactly 8 tasks."""
    assert len(dag.tasks) == 8, (
        f"Expected 8 tasks, got {len(dag.tasks)}"
    )


# ---------------------------------------------------------------------------
# Task existence tests
# ---------------------------------------------------------------------------

def test_all_required_tasks_exist(dag):
    """All 8 required task IDs must exist in the DAG."""
    expected_tasks = {
        "ingest_raw",
        "check_raw",
        "build_staging",
        "check_staging",
        "build_identity_layer",
        "check_identity",
        "build_mart",
        "check_mart",
    }
    actual_tasks = {task.task_id for task in dag.tasks}
    assert expected_tasks == actual_tasks, (
        f"Missing tasks: {expected_tasks - actual_tasks}"
    )


# ---------------------------------------------------------------------------
# Dependency tests
# ---------------------------------------------------------------------------

def test_check_raw_depends_on_ingest_raw(dag):
    """check_raw must run after ingest_raw."""
    check_raw = dag.get_task("check_raw")
    upstream_ids = {t.task_id for t in check_raw.upstream_list}
    assert "ingest_raw" in upstream_ids


def test_build_staging_depends_on_check_raw(dag):
    """build_staging must run after check_raw."""
    build_staging = dag.get_task("build_staging")
    upstream_ids = {t.task_id for t in build_staging.upstream_list}
    assert "check_raw" in upstream_ids


def test_check_staging_depends_on_build_staging(dag):
    """check_staging must run after build_staging."""
    check_staging = dag.get_task("check_staging")
    upstream_ids = {t.task_id for t in check_staging.upstream_list}
    assert "build_staging" in upstream_ids


def test_build_identity_depends_on_check_staging(dag):
    """build_identity_layer must run after check_staging."""
    build_identity = dag.get_task("build_identity_layer")
    upstream_ids = {t.task_id for t in build_identity.upstream_list}
    assert "check_staging" in upstream_ids


def test_check_identity_depends_on_build_identity(dag):
    """check_identity must run after build_identity_layer."""
    check_identity = dag.get_task("check_identity")
    upstream_ids = {t.task_id for t in check_identity.upstream_list}
    assert "build_identity_layer" in upstream_ids


def test_build_mart_depends_on_check_identity(dag):
    """build_mart must run after check_identity."""
    build_mart = dag.get_task("build_mart")
    upstream_ids = {t.task_id for t in build_mart.upstream_list}
    assert "check_identity" in upstream_ids


def test_check_mart_depends_on_build_mart(dag):
    """check_mart must run after build_mart."""
    check_mart = dag.get_task("check_mart")
    upstream_ids = {t.task_id for t in check_mart.upstream_list}
    assert "build_mart" in upstream_ids


def test_catchup_is_disabled(dag):
    """catchup must be disabled to prevent backfill on first run."""
    assert dag.catchup is False
