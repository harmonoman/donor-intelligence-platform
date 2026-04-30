import csv
from pathlib import Path

SAMPLE_PATH = Path("data/fec_sample.csv")
MAX_ROWS = 100_000

# These are the actual FEC itcont.txt column headers
# Confirmed from FEC data dictionary:
# https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/
REQUIRED_COLUMNS = [
    "NAME",
    "TRANSACTION_DT",
    "TRANSACTION_AMT",
]


def test_sample_file_exists():
    assert SAMPLE_PATH.exists(), (
        f"Sample file not found at {SAMPLE_PATH}. "
        "Run scripts/explore_fec.py first."
    )


def test_sample_row_count_under_limit():
    with open(SAMPLE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)  # skip header
        row_count = sum(1 for _ in reader)
    assert row_count < MAX_ROWS, (
        f"Sample has {row_count} rows — exceeds {MAX_ROWS} limit."
    )


def test_sample_row_count_is_meaningful():
    with open(SAMPLE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)  # skip header
        row_count = sum(1 for _ in reader)
    assert row_count > 10_000, (
        f"Sample only has {row_count} rows — too small to be meaningful."
    )


def test_delimiter_is_pipe():
    with open(SAMPLE_PATH, newline="", encoding="utf-8") as f:
        first_line = f.readline()
    assert "|" in first_line, (
        "Pipe delimiter not detected in first line. Check file format."
    )


def test_required_columns_present():
    with open(SAMPLE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        headers = next(reader)
    headers = [h.strip() for h in headers]
    missing = [col for col in REQUIRED_COLUMNS if col not in headers]
    assert not missing, f"Missing expected columns: {missing}"


def test_no_completely_empty_rows():
    with open(SAMPLE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)  # skip header
        empty_rows = sum(1 for row in reader if not any(field.strip() for field in row))
    assert empty_rows == 0, f"Found {empty_rows} completely empty rows in sample."
