# Data Quality Framework
## Epic 7: Data Quality Framework

---

## Raw Layer Quality Checks

Implemented in `pipelines/quality/check_raw.py`.

Three checks run immediately after raw ingestion:

| Check | Implementation | Notes |
|---|---|---|
| Row count | Partition must have at least 1 row | Catches failed ingestion |
| Required field nulls | Deferred to staging layer | See note below |
| Schema | All expected columns must exist | Catches FEC format changes |

### Note on Null Checks at Raw Layer

The raw layer stores FEC data exactly as received. Null validation
is intentionally deferred to the staging layer for the following reason:

Analysis of the 2024 cycle partition confirmed nulls exist in the
raw data across all three originally targeted fields:

| Field | Null Count | Pct of 15.2M rows |
|---|---|---|
| NAME | 114 | 0.001% |
| TRANSACTION_AMT | 1 | 0.000% |
| TRANSACTION_DT | 329 | 0.002% |

These nulls are expected behavior for a raw layer that stores data
exactly as received from the FEC. Filtering and null handling occur
in the staging layer where ENTITY_TP = IND filter removes non-individual
records and normalization handles edge cases.

### Expected Columns

The schema check validates these columns exist in `raw.fec_contributions`:
CMTE_ID, NAME, CITY, STATE, ZIP_CODE, EMPLOYER, OCCUPATION,
TRANSACTION_DT, TRANSACTION_AMT, SUB_ID, TRAN_ID, _load_date

---

## Staging Layer Quality Checks

Implemented in `pipelines/utils/pipeline_checks.py`.

Row count validation after staging build.

---

## Identity Layer Quality Checks

Implemented in `pipelines/utils/pipeline_checks.py`.

Row count validation after identity resolution.

---

## Mart Layer Quality Checks

Implemented in `pipelines/utils/pipeline_checks.py` and
`pipelines/quality/check_raw.py`.

Three checks after mart build:

| Check | Details |
|---|---|
| Row count | Mart must have at least 1 donor |
| No duplicate donor_ids | One row per donor enforced |
| No null engagement scores | All donors must have a score |

---

## Example Audit Query

```sql
SELECT
    execution_date,
    task_name,
    status,
    row_count_input,
    row_count_output,
    timestamp
FROM `project-id.metadata.pipeline_run_log`
WHERE status = 'FAIL'
ORDER BY timestamp DESC
```
