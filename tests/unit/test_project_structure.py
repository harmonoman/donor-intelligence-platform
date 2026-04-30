"""
Project structure validation test.
Ticket 1.4 — Project Directory Scaffold

Verifies that all required directories exist.
Fails fast if someone accidentally deletes or renames a core directory.
"""

from pathlib import Path

REQUIRED_DIRS = [
    "dags",
    "pipelines/ingest",
    "pipelines/staging",
    "pipelines/identity",
    "pipelines/marts",
    "pipelines/utils",
    "sql/raw",
    "sql/staging",
    "sql/core",
    "sql/marts",
    "tests/unit",
    "tests/integration",
    "tests/fixtures",
    "docs",
    "data",
    "scripts",
]


def test_required_directories_exist():
    """All pipeline directories must exist."""
    missing = [d for d in REQUIRED_DIRS if not Path(d).is_dir()]
    assert not missing, (
        "Missing required directories:\n"
        + "\n".join(f"  - {d}" for d in missing)
    )


def test_env_example_exists():
    """`.env.example` must exist and contain required keys."""
    env_example = Path(".env.example")
    assert env_example.exists(), ".env.example is missing"

    content = env_example.read_text()
    required_keys = [
        "GCP_PROJECT_ID",
        "GCP_DATASET_RAW",
        "AIRFLOW_HOME",
    ]
    missing = [k for k in required_keys if k not in content]
    assert not missing, f"Missing keys in .env.example: {missing}"


def test_gitkeep_files_exist():
    """Empty directories must have .gitkeep files so Git tracks them."""
    dirs_needing_gitkeep = [
        "pipelines/ingest",
        "pipelines/staging",
        "pipelines/identity",
        "pipelines/marts",
        "sql/raw",
        "sql/staging",
        "sql/core",
        "sql/marts",
        "tests/fixtures",
    ]
    missing = [
        d for d in dirs_needing_gitkeep
        if not Path(d, ".gitkeep").exists()
    ]
    assert not missing, (
        "Missing .gitkeep in:\n"
        + "\n".join(f"  - {d}" for d in missing)
    )
