"""
Shared environment utility.
Ticket 1.2 — GCP Project + BigQuery Environment Setup

Provides:
    - load_env(): loads .env file into os.environ
    - get_required_env(): gets a required env var or fails fast

Usage:
    from pipelines.utils.env import load_env, get_required_env
"""

import os
from pathlib import Path


def load_env(env_path: Path = Path(".env")) -> None:
    """
    Load .env file into environment variables.
    Shell environment takes precedence over .env values.
    Silent no-op if .env does not exist.
    """
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def get_required_env(var: str) -> str:
    """
    Get a required environment variable or fail fast with a clear message.
    """
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{var}' is not set.\n"
            f"Copy .env.example to .env and populate it."
        )
    return value
