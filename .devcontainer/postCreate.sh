#!/usr/bin/env bash
set -euo pipefail

echo "▶ Installing dependencies..."
uv sync

echo "▶ Verifying environment..."
uv run python --version

echo "▶ Loading environment variables..."
if [ -f /workspace/.env ]; then
    export $(grep -v '^#' /workspace/.env | xargs)
    echo "✅ Environment loaded from .env"
else
    echo "⚠️  No .env found — copy .env.example to .env and populate it"
fi

echo "✅ Environment ready."
