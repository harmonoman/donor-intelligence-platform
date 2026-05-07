# Donor Intelligence Platform

## Overview

The Donor Intelligence Platform is a batch-oriented data engineering system
that consolidates, cleans, and structures FEC individual contribution data
for donor re-engagement analysis and campaign targeting.

Built as a production-style capstone pipeline using:

- BigQuery (warehouse)
- Airflow (orchestration, local standalone in development)
- Python (normalization, ingestion, identity resolution, mart build)
- SQL (BigQuery transformations)
- pytest (test suite, 120 tests passing)

The system produces a single canonical donor analytics mart across
3,160,099 unique donors derived from 28 million FEC contribution records.

---

## Core Design Principles

- Deterministic outputs over probabilistic inference (MVP)
- Fully idempotent batch processing at every layer
- Fail-fast execution with loud validation gates
- Explicit identity resolution with no fuzzy matching in MVP
- Single source of truth per layer
- Single normalization utility shared across all layers
- Fully reproducible pipeline runs

---

## Data Source

### FEC Individual Contributions

Source: Federal Election Commission bulk data
Format: Pipe-delimited txt files, no header row, latin-1 encoding
Grain: One row per individual contribution event

Two full election cycles ingested:

| File | Cycle | Rows | Date Range |
|---|---|---|---|
| itcont_2024.txt | 2023-2024 | 15,264,403 | Aug 2023 to Nov 2024 |
| itcont_2026.txt | 2025-2026 | 12,737,488 | Feb 2025 to Mar 2026 |
| fec_sample.csv | 2025-2026 sample | 50,000 | Dec 2025 (test fixture) |

Total: approximately 28 million rows across three BigQuery partitions.

Key FEC fields used:

| FEC Column | Pipeline Name | Notes |
|---|---|---|
| SUB_ID | sub_id | Primary MERGE key, 100% populated |
| NAME | donor_name | Last, First format, inconsistent punctuation |
| ZIP_CODE | zip_normalized | Stripped to 5 digits, 0.1% null |
| CITY + STATE | donor_address_normalized | Combined for Rule 2 matching |
| TRANSACTION_AMT | contribution_amount | Dollars, includes negative refunds |
| TRANSACTION_DT | contribution_date | MMDDYYYY format, parsed to DATE |
| CMTE_ID | cmte_id | Raw committee ID, not enriched in MVP |
| ENTITY_TP | entity_type | Filtered to IND only in staging |

This dataset intentionally contains duplicates, inconsistent formatting,
and missing or partial fields. The pipeline handles all of these explicitly.

---

## Architecture
```
FEC txt files (local)
|
v
raw.fec_contributions      (partition overwrite, chunked 500k rows)
|
v
staging.stg_contributions  (normalized, filtered IND only, MERGE on SUB_ID)
|
v
core.dim_donors            (deterministic identity resolution, MERGE on SUB_ID)
core.dim_donors_unresolved (reserved for post-MVP collision handling)
|
v
marts.mart_donor_summary   (one row per donor, RFM scoring, full rebuild)
```

---

## Pipeline Scripts

Each script is independently runnable and parameterized with --execution-date
for Airflow compatibility:

```bash
# Raw ingestion (chunked for large files)
uv run python pipelines/ingest/load_raw_fec.py \
    --execution-date 2024-01-01 \
    --csv-path data/itcont_2024.txt \
    --chunked

# Staging normalization
uv run python pipelines/staging/build_staging.py \
    --execution-date 2024-01-01 \
    --chunked

# Identity resolution
uv run python pipelines/identity/build_identity.py \
    --execution-date 2024-01-01

# Mart build (full rebuild)
uv run python pipelines/marts/build_mart.py
```

---

## Airflow DAG Structure (Epic 6, Pending)

The pipeline is designed for a strictly ordered fail-fast DAG:
```
ingest_raw
|
check_raw
|
build_staging
|
check_staging
|
build_identity
|
check_identity
|
build_mart
|
check_mart
```

No downstream step executes unless upstream validation passes.
All scripts are parameterized and ready for DAG wiring.

---

## Data Layers

### 1. Raw Layer (raw.fec_contributions)

Direct ingestion from FEC source files. No transformation applied.
Partitioned by _load_date. Partition overwrite guarantees idempotency.

BigQuery partitions:

| Partition | Rows | Source |
|---|---|---|
| 2024-01-01 | 15,264,403 | itcont_2024.txt (2023-2024 cycle) |
| 2025-01-01 | 50,000 | fec_sample.csv (test fixture) |
| 2026-01-01 | 12,737,488 | itcont_2026.txt (2025-2026 cycle) |
| 2099-01-01 | 9 | identity fixture (integration tests) |

Chunked loading (500k rows per batch) is required for large files
due to container memory constraints (1.1GB available RAM).

---

### 2. Staging Layer (staging.stg_contributions)

Normalized, filtered, and typed records. MERGE on SUB_ID.

Transformations applied:
- Filter to ENTITY_TP = IND only (removes approximately 0.06% of records)
- normalize_name(): lowercase, strip punctuation, standardize format
- normalize_address(): lowercase city + state, strip punctuation
- normalize_zip(): strip to 5 digits, handle ZIP+4 formats
- Parse TRANSACTION_DT from MMDDYYYY to DATE
- Cast TRANSACTION_AMT to NUMERIC

Normalization contract: all normalization lives in pipelines/utils/normalize.py.
No normalization logic exists in SQL. This guarantees staging and identity
resolution use identical logic.

MERGE key: SUB_ID (confirmed 100% populated across 28M rows)

---

### 3. Identity Layer (core.dim_donors)

Deterministic matching assigns stable donor_id values across contributions.

Matching rules applied in strict order:

| Rule | Match Fields | Canonical Key Format |
|---|---|---|
| Rule 1 | donor_name_normalized + zip_normalized | name\|zip:XXXXX |
| Rule 2 | donor_name_normalized + donor_address_normalized | name\|addr:city state |
| No match | donor_name_normalized + sub_id | name\|id:SUB_ID |

donor_id = TO_HEX(MD5(canonical_key)), deterministic across all runs.

Match rule distribution on real data:

| Partition | rule1 | rule2 | no_match |
|---|---|---|---|
| 2024-01-01 | 15,245,657 | 8,828 | 771 |
| 2026-01-01 | 12,717,626 | 8,710 | 882 |

identity_conflict is always FALSE in the current batch hash implementation.
True collision detection requires incremental matching against an existing
donor registry. Tracked in backlog as Stretch Goal Phase 1.

dim_donors_unresolved exists with correct schema and is created on every run.
It is not populated in the current implementation. Reserved for post-MVP
collision handling.

---

### 4. Mart Layer (marts.mart_donor_summary)

One row per donor_id. Full rebuild on every run. No incremental logic.

Metrics per donor:

| Field | Description |
|---|---|
| contribution_count | Total number of contributions |
| total_contributions | Sum of all contribution amounts |
| first_donation_date | Earliest contribution date |
| last_donation_date | Most recent contribution date |
| days_since_last_donation | Days from last donation to dataset max date |
| committee_ids | Comma-separated list of committees donated to (capped at 50) |
| match_rule | Strongest identity match rule applied (MIN) |
| recency_score | 3=recent (0-90d), 2=active (91-365d), 1=lapsed (365+d) |
| frequency_score | 3=high (10+), 2=medium (2-9), 1=low (1) |
| monetary_score | 3=high ($1000+), 2=medium ($100-999), 1=low (under $100) |
| engagement_score | 0.4*recency + 0.3*frequency + 0.3*monetary |

Engagement thresholds grounded in real data distributions across
3,160,099 donors. See docs/mart-definitions.md for full analysis.

Current mart output:

| Engagement Tier | Donors | Pct |
|---|---|---|
| High (2.5+) | 327,211 | 10.4% |
| Medium (1.8-2.4) | 1,082,562 | 34.3% |
| Low (under 1.8) | 1,750,326 | 55.4% |

Reference date for recency: MAX(last_donation_date) across all donors.
CURRENT_DATE() is not used because FEC data is historical.

---

## Business Use Case

Identify lapsed major donors for re-engagement:

```sql
SELECT
    donor_name_normalized,
    zip_normalized,
    contribution_count,
    total_contributions,
    last_donation_date,
    days_since_last_donation,
    engagement_score
FROM `project-id.marts.mart_donor_summary`
WHERE total_contributions > 500
AND days_since_last_donation > 365
ORDER BY total_contributions DESC
```

Validated against real FEC data. Returns recognizable major donors
(Bloomberg, Griffin, Simons, Chan) at realistic contribution amounts
with correct lapsed status.

---

## Engagement Score Thresholds

Defined in pipelines/marts/thresholds.py.
Grounded in observed distributions. See docs/mart-definitions.md.

| Dimension | High | Medium | Low |
|---|---|---|---|
| Recency | 0-90 days (20%) | 91-365 days (24%) | 365+ days (56%) |
| Frequency | 10+ contributions (15%) | 2-9 (53%) | 1 (32%) |
| Monetary | $1,000+ (20%) | $100-$999 (53%) | Under $100 (26%) |

---

## Identity Resolution Known Limitations

1. Batch hash matching cannot distinguish same-donor-two-contributions
   from two-different-people-identical-name-and-ZIP. Same canonical key
   always resolves to same donor_id. Tracked as Stretch Goal Phase 1.

2. Donors contributing from multiple addresses receive separate donor_ids.
   Kenneth Griffin appears twice in the mart (ZIP 33131 Miami and
   ZIP 60611 Chicago). This is expected behavior of deterministic batch
   matching and is documented in docs/mart-definitions.md.

3. Rule 2 false merge risk: donors with the same name in the same city
   but no ZIP may merge incorrectly. Affects approximately 0.05% of
   records. Documented in docs/identity-resolution-fixtures.md.

---

## Stretch Goal Roadmap

### Phase 1: Incremental Collision Detection (Post-Demo)

Add a third SQL pass that evaluates new records against existing dim_donors.
Detects true collisions and populates dim_donors_unresolved.
Estimated effort: 3-5 days. No schema changes required.

### Phase 2: Bayesian Probabilistic Matching via Splink (Post-Demo)

Implements Fellegi-Sunter probabilistic record linkage for name variation
handling. Runs natively on BigQuery. Adds match_rule = rule4_splink.
Estimated effort: 1-2 weeks. No schema changes required.

See docs/ for the full engineering direction report.

---

## Stack Decisions: Capstone vs Production

This project uses locally-runnable equivalents of managed Google Cloud
services. The architecture mirrors production. The stack is adapted for
a capstone context without cloud billing or managed service provisioning.

| Component | Capstone | Production (Google Cloud) | Reason |
|---|---|---|---|
| Orchestration | Airflow standalone | Cloud Composer | Same DAGs, zero changes needed to deploy |
| Raw file storage | Local filesystem | Cloud Storage (GCS) | Avoids GCS setup and billing for capstone |
| Compute | Local Python scripts | Cloud Run Jobs | Same Python code, different host |
| Secrets | .env + ADC | Secret Manager + Workload Identity | ADC is the correct local GCP auth strategy |
| Data quality | Validation functions in runners | Dataplex | Same logic, declarative rules in production |
| CI/CD | Manual pytest | Cloud Build | Test suite is production-ready as written |
| Probabilistic matching | Splink (stretch goal) | Splink on Dataproc or Vertex AI | Same tool, managed infrastructure in production |
| Dashboards | Direct BigQuery queries | Looker Studio connected to mart | mart_donor_summary is BI-tool ready today |

What does not change between capstone and production: BigQuery as the
warehouse, SUB_ID as the MERGE key, normalize.py as the single
normalization source of truth, deterministic identity resolution SQL,
idempotent processing at every layer, the mart data model, and the
engagement scoring logic.

---

## Test Suite

120 tests passing across unit and integration layers.

| Layer | Tests | Coverage |
|---|---|---|
| Normalization | 31 | Real FEC edge cases including apostrophes, hyphens, ZIP+4 |
| Schema | 8 | Column names, types, required fields |
| Staging | 7 | Filter, normalization, date parsing, idempotency |
| Identity resolution | 13 | All 6 fixture scenarios including Rule 2 multi-ZIP MIN |
| Thresholds | 15 | Full coverage, no gaps, negative amount handling |
| Mart scoring | 16 | All tier combinations, formula weights, boundary values |
| BigQuery connection | 4 | Dataset existence, location |
| Raw ingestion | 7 | Load, idempotency, row count match |

Run with:

```bash
uv run pytest -v
```

---

## Project Structure
```
donor-intelligence-platform/
├── pipelines/
│   ├── ingest/          # Raw FEC ingestion
│   ├── staging/         # Normalization and staging build
│   ├── identity/        # Donor identity resolution
│   ├── marts/           # Analytics mart build and thresholds
│   └── utils/           # Shared normalization utility
├── sql/
│   ├── raw/             # Raw table schema
│   ├── staging/         # Staging SQL reference
│   ├── core/            # dim_donors schema reference
│   └── marts/           # Mart SQL and exploration queries
├── tests/
│   ├── unit/            # Pure logic tests
│   ├── integration/     # BigQuery integration tests
│   └── fixtures/        # Identity resolution fixture dataset (26 records)
├── docs/
│   ├── data-exploration.md
│   ├── mart-definitions.md
│   ├── identity-resolution-fixtures.md
│   └── gcp-setup.md
├── dags/                # Airflow DAGs (Epic 6 pending)
└── data/
├── fec_sample.csv   # 50k row test fixture
└── indiv_header_file.csv
```

---

## Non-MVP Scope

Explicitly excluded from current implementation:

- Fuzzy matching
- Probabilistic scoring (tracked as stretch goal)
- External enrichment APIs
- Streaming ingestion
- Multi-mart architecture
- ML-based scoring
- Committee name enrichment (CMTE_ID stored as raw ID)
- dbt

---

## Remaining Epics

| Epic | Status | Risk |
|---|---|---|
| Epic 6: Airflow DAG Wiring | Pending | Medium: untested end-to-end DAG execution |
| Epic 7: Data Quality Framework | Pending | Low: validation functions already exist in runners |
| Epic 8: Demo Readiness | Pending | Low: mart produces compelling real-data output |
