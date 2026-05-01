# FEC Data Exploration Findings
## Ticket 1.1 — Pre-Implementation Setup

> ⚠️ STATUS: COMPLETE — all placeholders resolved

**Completed:** April 29, 2025 \
**Engineer:** Mark \
**Source file:** itcont.txt \
**Source:** https://www.fec.gov/data/browse-data/?tab=bulk-data

---

## Source File Verification

| Property | Value |
|---|---|
| Filename | itcont.txt |
| Downloaded | April 29, 2025 |
| MD5 hash | 38bd918ccbc013a5f1b607e9ac4c45c5 |
| File size | 365.8 MB |
| Total rows | 1,980,629 |
| Skipped malformed rows | 0 |

---

## File Format

| Property | Value |
|---|---|
| Delimiter | pipe (`\|`) |
| Encoding | latin-1 |
| Header row | **No** — itcont.txt contains data from row 1 |
| Column count | 21 |
| Header source | `data/indiv_header_file.csv` (official FEC schema file) |

> **Important:** `itcont.txt` has no header row. Column names are supplied
> by `indiv_header_file.csv` which must be present for the exploration
> script and all downstream pipeline code to function correctly.

---

## Actual Column Names

As defined by `indiv_header_file.csv` and confirmed against FEC data dictionary:
```
CMTE_ID
AMNDT_IND
RPT_TP
TRANSACTION_PGI
IMAGE_NUM
TRANSACTION_TP
ENTITY_TP
NAME
CITY
STATE
ZIP_CODE
EMPLOYER
OCCUPATION
TRANSACTION_DT
TRANSACTION_AMT
OTHER_ID
TRAN_ID
FILE_NUM
MEMO_CD
MEMO_TEXT
SUB_ID 
```
---

## Column Name Mapping

| FEC Column Name | Pipeline Friendly Name | Notes |
|---|---|---|
| NAME | donor_name | Full name, Last First format, inconsistent punctuation |
| CITY | city | Occasionally null (273 / 1.98M rows) |
| STATE | state | 2-char abbreviation, rarely null |
| ZIP_CODE | zip_code | May include ZIP+4 extension, 0.1% null |
| EMPLOYER | employer | 1.7% null — not used in MVP matching |
| OCCUPATION | occupation | 1.7% null — not used in MVP matching |
| TRANSACTION_DT | contribution_date | Format: MMDDYYYY — requires parsing |
| TRANSACTION_AMT | contribution_amount | Integer cents or dollars — verify format |
| SUB_ID | source_record_id | **100% populated — confirmed primary MERGE key** |
| TRAN_ID | transaction_id | Present but not used as MERGE key — SUB_ID preferred |
| CMTE_ID | committee_id | Committee receiving contribution |
| MEMO_CD | memo_code | 99.3% null — not used in MVP |
| MEMO_TEXT | memo_text | 48.8% null — not used in MVP |
| OTHER_ID | other_id | 37.2% null — not used in MVP |

---

## SUB_ID Reliability Assessment

| Property | Value |
|---|---|
| Populated | 1,980,629 / 1,980,629 (100.0%) |
| Decision | **Primary MERGE key** |

**Rationale:** SUB_ID is 100% populated across all 1.98M rows with zero
nulls. This is a unique submission identifier assigned by the FEC. It is
the most reliable key available and eliminates the need for the fallback
key in staging.

**Impact on implementation:** The fallback MERGE key
(`NAME_normalized + TRANSACTION_DT + TRANSACTION_AMT`) is no longer needed
for this dataset. SUB_ID will be used as the primary key in all staging
MERGE operations.

---

## Null Analysis

| Column | Null Count | Null Rate | MVP Impact |
|---|---|---|---|
| MEMO_CD | 1,966,675 | 99.3% | None — not used in MVP |
| MEMO_TEXT | 966,167 | 48.8% | None — not used in MVP |
| OTHER_ID | 737,159 | 37.2% | None — not used in MVP |
| TRANSACTION_PGI | 163,561 | 8.3% | Low — not used in MVP |
| EMPLOYER | 33,113 | 1.7% | Low — not used in MVP matching |
| OCCUPATION | 32,928 | 1.7% | Low — not used in MVP matching |
| ZIP_CODE | 1,532 | 0.1% | **Medium — required for identity resolution Rule 1** |
| STATE | 357 | 0.0% | Low |
| CITY | 273 | 0.0% | Low |
| NAME | 0 | 0.0% | ✅ Clean — critical for identity resolution |
| TRANSACTION_AMT | 0 | 0.0% | ✅ Clean — critical for mart |
| TRANSACTION_DT | 0 | 0.0% | ✅ Clean — critical for mart |
| SUB_ID | 0 | 0.0% | ✅ Clean — primary MERGE key |

**Key finding:** The three fields most critical to this pipeline
(NAME, TRANSACTION_AMT, TRANSACTION_DT) have zero nulls.
ZIP_CODE has a 0.1% null rate — records missing ZIP will fall through
to Rule 2 (name + full address) in identity resolution.

---

## Duplicate Analysis

| Property | Value |
|---|---|
| Unique NAME + DATE + AMOUNT combinations | 1,507,162 |
| Keys appearing more than once | 214,894 |
| Duplication rate | ~10.8% of unique key combinations |

**Example duplicate records:**

| NAME | DATE | AMOUNT | Occurrences |
|---|---|---|---|
| HARRIS, JANET P | 12132025 | $0 | 6x |
| HARRIS, JANET P | 12132025 | $1 | 2x |
| DUNN, PEGGY | 12132025 | $2 | 3x |
| DUNCAN, CHARLES | 12132025 | $23 | 2x |
| CARLSON, MARY | 12132025 | $10 | 4x |

**Interpretation:** These duplicates represent multiple contributions
from the same donor on the same date for the same amount. This is a
known FEC data pattern — amended filings, split contributions, and
recurring small-dollar donations all produce identical
NAME + DATE + AMOUNT keys.

**Implication for MERGE key:** This confirms that
`NAME + TRANSACTION_DT + TRANSACTION_AMT` alone is not a reliable
unique key. SUB_ID as primary MERGE key is the correct decision —
each SUB_ID is unique even when the contribution details appear identical.

---

## Sample Definition

| Property | Value |
|---|---|
| Sample method | First 50,000 rows of itcont.txt |
| Sample row count | 50,000 |
| Output file | `data/fec_sample.csv` |
| Header row | Yes — written by exploration script from indiv_header_file.csv |
| Encoding | utf-8 |
| Reproducible command | See below |

```bash
uv run python scripts/explore_fec.py \
  --input data/itcont.txt \
  --headers data/indiv_header_file.csv \
  --rows 50000
```

> **Note:** Sample reproducibility depends on using the same source file.
> Verify using the MD5 hash above before regenerating the sample.

---

## MERGE Key Decision

**Final decision: SUB_ID as primary and only MERGE key**

| Property | Value |
|---|---|
| Primary key | `SUB_ID` |
| Fallback key | Not needed — SUB_ID is 100% populated |
| Known limitation | None for this dataset |

The fallback key (`NAME_normalized + TRANSACTION_DT + TRANSACTION_AMT`)
documented in the README is retained as a design pattern for future
datasets where SUB_ID may not be available, but will not be implemented
in MVP.

---

## Key Findings for Implementation

- **SUB_ID is 100% reliable** — simplifies staging MERGE significantly.
  No fallback key logic needed for this dataset.
- **NAME has zero nulls** — identity resolution can proceed without
  null-handling guards on the primary match field.
- **ZIP_CODE is 0.1% null** — 1,532 records will fall through Rule 1
  (name + ZIP) to Rule 2 (name + full address) in identity resolution.
  This is expected and handled by the matching strategy.
- **TRANSACTION_DT is in MMDDYYYY format** — requires explicit date
  parsing in staging. Not ISO format. Staging SQL must handle this.
- **~10.8% of NAME + DATE + AMOUNT combinations appear more than once** —
  confirms that SUB_ID is necessary as the MERGE key. The fallback key
  would produce incorrect deduplication on this data.

---

## Column Mapping — FEC to Pipeline

This table maps actual FEC column names to their semantic meaning
in the pipeline. Renaming happens in the staging layer — the raw
layer uses FEC names exactly.

| FEC Column | Type | Semantic Meaning | MVP Usage |
|---|---|---|---|
| CMTE_ID | STRING | Committee receiving contribution | Context only |
| AMNDT_IND | STRING | Amendment indicator (N/A/T) | Not used in MVP |
| RPT_TP | STRING | Report type | Not used in MVP |
| TRANSACTION_PGI | STRING | Primary/general indicator | Not used in MVP |
| IMAGE_NUM | STRING | FEC filing image reference | Not used in MVP |
| TRANSACTION_TP | STRING | Transaction type code | Not used in MVP |
| ENTITY_TP | STRING | Entity type (IND = individual) | Filter reference |
| NAME | STRING | Donor full name (Last, First) | Identity resolution |
| CITY | STRING | Donor city | Address matching |
| STATE | STRING | Donor state (2-char) | Address matching |
| ZIP_CODE | STRING | Donor ZIP (may include ZIP+4) | Identity Rule 1 |
| EMPLOYER | STRING | Donor employer | Stored, not matched |
| OCCUPATION | STRING | Donor occupation | Stored, not matched |
| TRANSACTION_DT | STRING | Contribution date (MMDDYYYY) | Mart metrics |
| TRANSACTION_AMT | NUMERIC | Contribution amount (dollars) | Mart metrics |
| OTHER_ID | STRING | Other ID (37.2% null) | Not used in MVP |
| TRAN_ID | STRING | FEC transaction ID | Reference only |
| FILE_NUM | STRING | FEC file number | Reference only |
| MEMO_CD | STRING | Memo code (99.3% null) | Not used in MVP |
| MEMO_TEXT | STRING | Memo text (48.8% null) | Not used in MVP |
| SUB_ID | STRING | Submission ID — primary MERGE key | MERGE key |
| _load_date | DATE | Pipeline load date (added by us) | Partitioning |

---

## Staging Prerequisites

These are known data characteristics that MUST be handled in staging
transformations. Documented here to prevent silent errors downstream.

### ZIP_CODE Cleaning Required
FEC ZIP codes frequently appear in non-standard formats:
- `302011234` — ZIP+4 without hyphen
- `3021` — truncated ZIP
- `30201-1234` — ZIP+4 with hyphen

Staging must strip to 5-digit ZIP before identity resolution Rule 1
(name + ZIP) can match reliably. Use: `LEFT(ZIP_CODE, 5)`

### ENTITY_TP Filter Required
FEC individual contribution files contain records for multiple entity types:
- `IND` — individual donor (target for this pipeline)
- `ORG` — organization
- `CAN` — candidate
- `COM` — committee

Staging must filter to `ENTITY_TP = 'IND'` before building
`stg_contributions`. Failure to filter will introduce non-individual
records into identity resolution and the mart.

---

## Open Questions

- **TRANSACTION_DT date parsing:** Confirmed format is `MMDDYYYY`.
  Staging SQL must use `PARSE_DATE('%m%d%Y', TRANSACTION_DT)` to convert
  to a standard DATE type. BigQuery does not handle this format natively.

- **TRANSACTION_AMT format:** Confirm whether values are in dollars or
  cents before writing staging SQL.
