#!/usr/bin/env bash
set -euo pipefail

echo "▶ Installing dependencies..."
uv sync

echo "✅ Done. Python: $(uv run python --version)"
