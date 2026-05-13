# Donor Intelligence Platform: Demo Script
## Ticket 8.2: End-to-End Demo

---

## Demo Overview

This demo runs the full Donor Intelligence Platform pipeline from raw
ingestion to analytics mart and shows a real fundraising use case:
identifying high-value lapsed donors for re-engagement.

What happens in order:

1. Pipeline triggers in Airflow
2. Raw FEC data is ingested, cleaned, and normalized
3. Donor identities are resolved deterministically
4. Analytics mart is built with RFM engagement scoring
5. Quality checks validate every layer
6. Lapsed major donors are surfaced by name and contribution total

Expected runtime: under 10 minutes on the 50k sample dataset.

---

## Pre-Demo Checklist

Complete all of these before starting. Nothing should be set up during
the demo itself.

- [ ] Airflow is running and UI is accessible at http://localhost:8080
- [ ] GCP credentials are fresh (run `gcloud auth application-default login` if unsure)
- [ ] Environment variables are set:
```bash
  echo $GCP_PROJECT_ID
```
  If empty, set them and restart Airflow:
```bash
  export GCP_PROJECT_ID=project-a10238bd-a355-474b-b6a
  export GOOGLE_CLOUD_PROJECT=project-a10238bd-a355-474b-b6a
  airflow standalone
```
- [ ] BigQuery datasets exist: raw, staging, core, marts, metadata
- [ ] Sample dataset is available: `data/fec_sample.csv` (50,000 rows)
- [ ] Test suite passes:
```bash
  uv run pytest -v
```
- [ ] donor_pipeline DAG appears in Airflow UI (search for "donor")

If any item fails, resolve it before starting. Do not troubleshoot
during the demo.

---

## Step 1: Trigger the Pipeline

### Option A: Airflow UI (recommended for demo)

1. Open http://localhost:8080 in your browser
2. Search for `donor_pipeline` in the DAG list
3. Click the toggle to unpause the DAG if it shows as paused
4. Click the play button (triangle icon) on the right side
5. Select **Trigger DAG w/ config**
6. In the JSON config box enter:
```json
   {
       "csv_path": "data/fec_sample.csv"
   }
```
7. Set the logical date to today's date at 00:00:00
8. Click **Trigger**

### Option B: CLI (backup only)

Use this option only if the Airflow UI is unavailable. The UI is
strongly preferred for live demos because it shows task progress visually.

```bash
airflow dags trigger donor_pipeline \
    --conf '{"csv_path": "data/fec_sample.csv"}'
```

Note: Airflow assigns the current timestamp as the logical date
automatically. If the DAG has already been triggered today, this
command will fail with a duplicate logical date error. Use a future
date to avoid conflicts:

```bash
airflow dags trigger donor_pipeline \
    --conf '{"csv_path": "data/fec_sample.csv"}' \
    --logical-date 2025-06-01T00:00:00+00:00
```

Note: use Option A for the demo. The UI shows task progress visually
which is more compelling for an audience.

---

## Step 2: Observe Pipeline Execution

Click into the DAG run to open the task graph view.

Watch for:

- Tasks turn from grey (queued) to yellow (running) to green (success)
- Tasks execute in strict order, one at a time:
ingest_raw        loads 50,000 FEC contribution records
check_raw         validates row count and schema
build_staging     normalizes names, ZIPs, dates
check_staging     validates row count consistency and null rates
build_identity_layer  assigns stable donor_ids
check_identity    validates sub_id uniqueness and reconciliation
build_mart        builds RFM-scored donor summary
check_mart        validates grain, null scores, row count

- If any check task fails, all downstream tasks show as upstream-failed
  and stop immediately. This is fail-fast behavior working correctly.

Expected runtime: 8 to 10 minutes for the full 8-task sequence on the
50k sample.

---

## Step 3: Validate Successful Run

Once all 8 tasks show green, confirm the audit trail in BigQuery.

Run this query in the BigQuery console:

```sql
SELECT
    run_id,
    task_name,
    status,
    row_count_output,
    timestamp
FROM `project-a10238bd-a355-474b-b6a.metadata.pipeline_run_log`
ORDER BY timestamp DESC
LIMIT 8
```

Expected output: 8 rows, all with status = PASS, one per task, in
reverse chronological order.

| task_name | status | row_count_output |
|---|---|---|
| check_mart | PASS | 3,160,102 |
| build_mart | PASS | 3,160,102 |
| check_identity | PASS | 49,981 |
| build_identity_layer | PASS | 49,981 |
| check_staging | PASS | 49,981 |
| build_staging | PASS | 49,981 |
| check_raw | PASS | 50,000 |
| ingest_raw | PASS | 50,000 |

Note: mart row count reflects the full pre-loaded dataset (3.16M donors),
not just the 50k sample run. The mart is a full rebuild on every run.

---

## Step 4: Run the Lapsed Donor Query

Run this query in the BigQuery console:

```sql
SELECT
    donor_name_normalized,
    zip_normalized,
    contribution_count,
    total_contributions,
    last_donation_date,
    days_since_last_donation,
    engagement_score
FROM `project-a10238bd-a355-474b-b6a.marts.mart_donor_summary`
WHERE total_contributions > 500
AND days_since_last_donation > 365
ORDER BY total_contributions DESC
LIMIT 10
```

Expected results include recognizable major donors from real FEC data:

| donor | total | last gift | days lapsed | score |
|---|---|---|---|---|
| mellon timothy | $56,010,300 | 2025-03-04 | 389 | 1.9 |
| griffin kenneth c | $20,000,000 | 2023-12-28 | 821 | 1.9 |
| bloomberg michael | $19,000,000 | 2024-05-30 | 667 | 1.6 |
| simons james | $5,013,200 | 2023-12-14 | 835 | 1.9 |
| chan michelle | $5,000,000 | 2024-08-15 | 590 | 1.6 |

These are real people. These are real contribution amounts. This is
real FEC data.

---

## Step 5: Tell the Story

Use this narrative when presenting results.

**The business problem:**
A fundraising organization has millions of donation records but no
reliable way to identify which major donors have gone quiet. Manually
reviewing contribution history for 3 million donors is not feasible.

**What the pipeline does:**
It ingests 28 million FEC contribution records, resolves 3.16 million
unique donor identities using deterministic matching, and scores every
donor on three dimensions: how recently they gave, how often they gave,
and how much they gave in total.

**What the query shows:**
Timothy Mellon gave $56 million and has not donated in 389 days. His
engagement score is 1.9 out of 3.0. High monetary value, low recency.
This is exactly the profile of a donor worth a personal re-engagement
call from a senior fundraiser.

Michael Bloomberg gave $19 million and has been lapsed for 667 days.
His score is 1.6. Same pattern. Same priority.

**Why this matters:**
Acquiring a new donor costs significantly more than re-engaging a lapsed
one. These donors already said yes. The pipeline identifies them
automatically, at scale, every time it runs.

**The technical story:**
The pipeline runs on real FEC data. Every step is validated by a quality
gate. The entire system is idempotent: run it twice, get the same result.
The Airflow DAG enforces fail-fast behavior: if any validation fails,
the pipeline stops immediately and nothing downstream is affected.

---

## Performance Expectations

| Dataset | Expected Runtime |
|---|---|
| 50k sample (fec_sample.csv) | 8 to 10 minutes |
| 2024 cycle (15.2M rows) | 45 to 90 minutes |
| 2026 cycle (12.7M rows) | 40 to 75 minutes |

For live demos, always use the 50k sample. Show the full 28M row results
as pre-computed output already in BigQuery.

---

## Troubleshooting Quick Checks

**DAG not appearing in Airflow UI**

Verify dags_folder is pointing to the correct directory:
```bash
grep "dags_folder" ~/airflow/airflow.cfg
```
It must show `/workspace/dags`. If not, see docs/airflow-setup.md.

**Tasks failing with GCP auth errors**

Credentials have expired. Run:
```bash
gcloud auth application-default login
```
Then restart Airflow with the environment variables set.

**Tasks failing with GCP_PROJECT_ID not found**

The environment variable is not reaching Airflow workers. Verify:
```bash
cat ~/airflow/.env
```
It must contain `GCP_PROJECT_ID=project-a10238bd-a355-474b-b6a`.

**Lapsed donor query returns no results**

The mart may not have been rebuilt recently. Trigger a fresh DAG run
or run the mart build manually:
```bash
uv run python pipelines/marts/build_mart.py
```

**Duplicate logical date error when triggering**

Each logical date can only be used once per DAG run. Use a date that
has not been used before. Today's date works unless the DAG was already
triggered today.
