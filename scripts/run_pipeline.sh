#!/bin/bash
set -e

DATE=$1
CSV=$2

if [ -z "$DATE" ] || [ -z "$CSV" ]; then
    echo "Usage: ./scripts/run_pipeline.sh <execution-date> <csv-path>"
    echo "Example: ./scripts/run_pipeline.sh 2024-01-01 data/itcont_2024.txt"
    exit 1
fi

echo "Running pipeline for date: $DATE file: $CSV"

uv run python pipelines/ingest/load_raw_fec.py --execution-date $DATE --csv-path $CSV --chunked
uv run python pipelines/staging/build_staging.py --execution-date $DATE --chunked
uv run python pipelines/identity/build_identity.py --execution-date $DATE
uv run python pipelines/marts/build_mart.py --execution-date $DATE

echo "Pipeline complete for $DATE"
