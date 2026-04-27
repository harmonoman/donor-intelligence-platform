#!/usr/bin/env bash
set -euo pipefail

echo "▶ Installing dependencies..."
uv sync

echo "▶ Verifying environment..."
uv run python --version

echo "✅ Environment ready."