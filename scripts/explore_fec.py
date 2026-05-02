"""
FEC Data Exploration Script
Ticket 1.1 — Pre-Implementation Setup

Purpose:
    Inspect the real FEC itcont.txt file, document its structure,
    and produce a reproducible sample for pipeline development.

Usage:
    uv run python scripts/explore_fec.py \
        --input data/itcont.txt \
        --headers data/indiv_header_file.csv \
        --rows 50000
"""

import argparse
import csv
import hashlib
from collections import Counter
from pathlib import Path

DELIMITER = "|"
ENCODING = "latin-1"
OUTPUT_SAMPLE = Path("data/fec_sample.csv")


def parse_args():
    parser = argparse.ArgumentParser(description="Explore and sample FEC itcont.txt")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/itcont.txt"),
        help="Path to raw FEC itcont.txt file",
    )
    parser.add_argument(
        "--headers",
        type=Path,
        default=Path("data/indiv_header_file.csv"),
        help="Path to FEC header file (indiv_header_file.csv)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Number of rows to include in sample (default: 50000)",
    )
    return parser.parse_args()


def explore(input_path: Path, header_path: Path, sample_rows: int):
    OUTPUT_SAMPLE.parent.mkdir(exist_ok=True)

    print(f"\n{'='*60}")
    print("FEC DATA EXPLORATION REPORT")
    print(f"{'='*60}\n")

    # Validate input files exist
    if not input_path.exists():
        print(f"ERROR: Data file not found at {input_path}")
        print("Download from: https://www.fec.gov/data/browse-data/?tab=bulk-data")
        print("Individual Contributions → itcont.txt")
        return

    if not header_path.exists():
        print(f"ERROR: Header file not found at {header_path}")
        print("Expected: data/indiv_header_file.csv")
        return

    # MD5 hash for reproducibility verification
    file_hash = hashlib.md5(open(input_path, "rb").read()).hexdigest()
    print(f"Source file : {input_path}")
    print(f"File size   : {input_path.stat().st_size / 1_000_000:.1f} MB")
    print(f"Source MD5  : {file_hash}\n")

    # Load column names from the FEC header file
    # indiv_header_file.csv is comma-delimited, itcont.txt is pipe-delimited
    with open(header_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",")
        headers = [col.strip() for col in next(reader)]

    print(f"Headers loaded from : {header_path}")
    print("Delimiter           : pipe (|) confirmed")
    print(f"Column count        : {len(headers)}")
    print("\nColumns:\n")
    for i, col in enumerate(headers):
        print(f"  {i:02d}. {col}")

    # Collect sample rows + stats
    # itcont.txt has NO header row — data starts at line 1
    sample = []
    null_counts = Counter()
    total_rows = 0
    sub_id_populated = 0
    duplicate_check = Counter()  # Note: held in memory — fine for samples, not full file
    skipped_rows = 0

    with open(input_path, newline="", encoding=ENCODING) as f:
        reader = csv.reader(f, delimiter=DELIMITER)
        # No next(reader) — itcont.txt has no header row

        for row in reader:
            total_rows += 1

            if len(row) != len(headers):
                skipped_rows += 1
                continue

            row_dict = dict(zip(headers, [v.strip() for v in row]))

            # Track nulls
            for col in headers:
                if not row_dict.get(col):
                    null_counts[col] += 1

            # Track SUB_ID population
            if row_dict.get("SUB_ID"):
                sub_id_populated += 1

            # Track potential duplicates (NAME + TRANSACTION_DT + TRANSACTION_AMT)
            dup_key = (
                row_dict.get("NAME", ""),
                row_dict.get("TRANSACTION_DT", ""),
                row_dict.get("TRANSACTION_AMT", ""),
            )
            duplicate_check[dup_key] += 1

            # Collect sample
            if len(sample) < sample_rows:
                sample.append(row_dict)

    # Report
    print(f"\nTotal rows in full file : {total_rows:,}")
    print(f"Skipped malformed rows  : {skipped_rows:,}")
    print(f"Sample rows collected   : {len(sample):,}")

    print("\n--- SUB_ID Analysis ---")
    sub_id_pct = (sub_id_populated / total_rows * 100) if total_rows else 0
    print(f"SUB_ID populated: {sub_id_populated:,} / {total_rows:,} ({sub_id_pct:.1f}%)")
    if sub_id_pct > 95:
        print("✅ SUB_ID is reliable — use as primary MERGE key")
    elif sub_id_pct > 50:
        print("⚠️  SUB_ID partially populated — use as preferred key with fallback")
    else:
        print("❌ SUB_ID unreliable — use fallback MERGE key only")

    print("\n--- Null Analysis (top 10 most null columns) ---")
    for col, count in null_counts.most_common(10):
        pct = count / total_rows * 100
        print(f"  {col:<20} {count:>8,} nulls ({pct:.1f}%)")

    print("\n--- Duplicate Analysis ---")
    duplicates = {k: v for k, v in duplicate_check.items() if v > 1}
    print(f"Unique key combinations   : {len(duplicate_check):,}")
    print(f"Keys appearing > once     : {len(duplicates):,}")
    if duplicates:
        print("\nExample duplicate records (NAME | DATE | AMOUNT):")
        for key, count in list(duplicates.items())[:5]:
            print(f"  {key[0][:30]:<30} | {key[1]} | ${key[2]} — appears {count}x")

    # Write sample — with headers (we supply them since itcont.txt has none)
    print("\n--- Writing Sample ---")
    with open(OUTPUT_SAMPLE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="|")
        writer.writeheader()
        writer.writerows(sample)
    print(f"✅ Sample written to {OUTPUT_SAMPLE}")
    print(f"   Rows     : {len(sample):,}")
    print(f"   Columns  : {len(headers)}")

    print(f"\n{'='*60}")
    print("NEXT STEPS")
    print(f"{'='*60}")
    print("1. Review output above carefully")
    print("2. Fill in docs/data-exploration.md with your findings")
    print("3. Run: uv run pytest tests/unit/test_fec_sample.py")
    print("4. All tests should pass ✅")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    args = parse_args()
    explore(args.input, args.headers, args.rows)
