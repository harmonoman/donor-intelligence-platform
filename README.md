# Donor Intelligence Platform

## Overview

The Donor Intelligence Platform is a batch-oriented data engineering system that simulates how fundraising organizations consolidate, clean, and structure donor data for campaign targeting and re-engagement analysis.

It is designed as a production-style pipeline using:

- BigQuery (warehouse)
- Airflow (orchestration)
- SQL (transformations)
- Python (light validation logic)

The system produces a **single canonical donor analytics mart**.

---

## Core Design Principles

- Deterministic outputs over probabilistic inference (MVP)
- Fully idempotent batch processing
- Fail-fast execution at every data layer
- Explicit identity resolution (no fuzzy matching in MVP)
- Single source of truth per layer
- Fully reproducible pipeline runs

---

## Data Source

### FEC Individual Contributions (Sample Dataset)

- Source: Federal Election Commission donation records
- Format: CSV extract (sample subset for reproducibility)
- Grain: individual contribution event

Fields include:
- donor_name
- donor_address
- city/state/zip
- contribution_amount
- contribution_date
- (optional if available) source_record_id

This dataset intentionally contains:
- duplicates
- inconsistent formatting
- missing or partial fields

---

## Architecture

```
CSV (FEC Sample)
↓
Airflow DAG
↓
BigQuery Raw Layer
↓
Staging Layer (cleaned data)
↓
Identity Layer (dim_donors)
↓
Analytics Mart (mart_donor_summary)
```

---

## Airflow DAG Structure (FAIL-FAST ORDER)

The pipeline uses a **strictly ordered, interleaved validation design**:

1. ingest_raw
2. check_raw
3. build_staging
4. check_staging
5. build_identity_layer
6. check_identity
7. build_mart
8. check_mart

### Key Principle

> No downstream step executes unless upstream validation passes.

This ensures true fail-fast behavior and prevents bad data propagation.

---

## Data Layers

### 1. Raw Layer (raw.fec_contributions)

- Direct ingestion from CSV
- Partition overwrite per execution date

**Idempotency**
- Safe re-runs via partition replacement
- No accumulation of duplicates

---

### 2. Staging Layer (stg_* tables)

- Cleaned + standardized records
- Normalization applied consistently

#### Normalization Contract (Critical)

- lowercase transformation
- punctuation removal
- abbreviation standardization (St, Ave, Rd, etc.)
- consistent formatting across all layers

#### Idempotency Strategy

- MERGE-based updates

#### MERGE Key

Preferred:
- source_record_id (if available)

Fallback:
- donor_name_normalized + contribution_date + contribution_amount

⚠️ Note: fallback key may produce collisions in rare duplicate contribution scenarios (accepted MVP limitation)

---

### 3. Identity Layer (dim_donors)

#### Matching Strategy (Deterministic Only)

Ordered rules:

1. normalized_name + ZIP
2. normalized_name + full_address

If no match:
- new donor_id created

#### Collision Handling

If multiple matches exist for a record:

- identity_conflict = TRUE
- record stored in:
  - dim_donors (non-merged duplicate representation)
  - dim_donors_unresolved (audit + review table)

If Rule 2 matches multiple existing donors:
- treated as collision
- routed to dim_donors_unresolved

No silent merges occur.

---

## 4. Mart Layer (mart_donor_summary)

### Grain

One row per donor_id

---

### Engagement Score (MVP Definition)

A deterministic weighted composite:

- Recency Score → 40%
- Frequency Score → 30%
- Monetary Score → 30%

### Metrics

- recency_score: days since last donation (bucketed)
- frequency_score: number of contributions (bucketed)
- monetary_score: total contribution sum (bucketed)

Final field:

engagement_score = 0.4 * recency + 0.3 * frequency + 0.3 * monetary


---

### Business Use Case

Used to identify:

> Lapsed donors who previously contributed $500+ and have not donated in the last 12 months

---

## Data Quality Framework

Each stage includes validation gates:

- row count checks
- null thresholds on critical fields
- uniqueness constraints (donor_id)
- schema consistency validation

### Failure Behavior

- Any failed check halts pipeline execution immediately
- No downstream transformations execute after failure
- No partial mart updates are allowed

---

## Idempotency Model

### Guaranteed Safe Re-runs

- Raw → partition overwrite
- Staging → MERGE
- Identity → full deterministic rebuild
- Mart → full rebuild from identity layer

---

## Metadata Tracking

### BigQuery Table: pipeline_run_log

Fields:

- run_id
- execution_date
- task_name
- row_count_input
- row_count_output
- status (PASS/FAIL)
- timestamp

### Behavior

- append-only (no updates)
- full historical lineage preserved
- safe across re-runs

---

## Schema Evolution Strategy

- Raw layer absorbs upstream changes
- Staging explicitly selects required columns
- Unknown fields ignored unless mapped
- Breaking changes fail loudly (no silent drift)

---

## Operational Assumptions

- Daily Airflow schedule
- Backfills executed via explicit DAG run with execution_date parameter
- Logging only (no alerting in MVP)

---

## Non-MVP Scope

Explicitly excluded:

- fuzzy matching
- external enrichment APIs
- streaming ingestion
- multi-mart architecture
- ML-based scoring

---

## Summary

FEC raw data → staging normalization → deterministic identity resolution → donor mart → campaign-ready segmentation
