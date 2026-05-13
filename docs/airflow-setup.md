# Airflow Setup Guide
## Donor Intelligence Platform — Ticket 1.3

> ⚠️ STATUS: COMPLETE \
> Last updated: April 29, 2025 \
> Author: Mark \

---

## Decision

**Airflow Standalone** — runs inside the existing dev container.

Chosen for speed, simplicity, and MVP discipline. The DAG logic is
identical regardless of deployment model. Compose adds operational
complexity with no learning benefit at this stage.

---

## Prerequisites

- Dev container running
- `uv` environment configured
- `bash` shell (not `sh`)

---

## Step 1 — Install Airflow

Airflow requires a constrained install due to its dependency footprint.
Run inside the dev container in a `bash` shell:

```bash
bash

AIRFLOW_VERSION=2.9.1
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

uv pip install "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}"
```

---

## Step 2 — Configure Environment

Add to `.env`:

```bash
AIRFLOW_HOME=/workspace/airflow
```

Load it:

```bash
source ~/.bashrc
export AIRFLOW_HOME=/workspace/airflow
```

Create the DAGs directory:

```bash
mkdir -p airflow
mkdir -p dags
```

---

## Step 3 — Run Airflow Standalone

```bash
airflow standalone
```

This single command:
- Initializes the metadata database
- Creates an admin user
- Starts the webserver and scheduler

Watch the output for a line like:
standalone | Airflow is ready
standalone | Login with username: admin  password: <generated>
standalone | Airflow Standalone is for development purposes only.

Note the generated password — you will need it to log into the UI.

---

## Step 4 — Access the UI

Open in your browser:
```
http://localhost:8080
```

Login with:
- Username: `admin`
- Password: from the terminal output above

### Port Forwarding in VS Code

If the browser shows a blank page or connection refused:

1. In VS Code open the **Ports** tab (bottom panel)
2. Confirm port `8080` appears — VS Code usually detects it automatically
3. If not, click **Forward a Port** and add `8080`
4. Click the globe icon next to port `8080` to open in browser

---

## Step 5 — Verify the DAG

Open a second terminal and run:

```bash
# Load environment in the new session
bash
export $(grep -v '^#' .env | xargs)

# Confirm DAG is discoverable
airflow dags list

# Confirm no import errors
airflow dags list-import-errors

# Confirm tasks are defined
airflow tasks list hello_world

# Run DAG without scheduler (test mode)
airflow dags test hello_world
```

---

## Step 6 — Trigger via UI

1. Open http://localhost:8080
2. Find `hello_world` in the DAG list
3. Click the toggle to unpause it
4. Click the play button → **Trigger DAG**
5. Click into the DAG run → confirm both tasks show green

---

## Gitignore Entries

The following Airflow artifacts are gitignored:

```
airflow/
logs/
airflow.db
```

The `dags/` directory IS committed — it contains your pipeline code.

---

## Troubleshooting

**`airflow: command not found`**
Run `bash` first — the `sh` shell does not source `.bashrc`.

**DAG not appearing in UI**
Check for import errors:
```bash
airflow dags list-import-errors
```

**UI not loading at localhost:8080**
Confirm `airflow standalone` is still running in the first terminal.
Port 8080 must be forwarded in VS Code — check the Ports tab.

**Password lost**
Reset it:
```bash
airflow users reset-password --username admin
```

---

## donor_pipeline DAG

### Overview

The `donor_pipeline` DAG orchestrates the full pipeline in strict fail-fast order:
ingest_raw
|
check_raw
|
build_staging
|
check_staging
|
build_identity_layer
|
check_identity
|
build_mart
|
check_mart

Each check task validates the output of the previous build task. If any
check fails, all downstream tasks are skipped immediately. No bad data
propagates to the next layer.

### Configuration

The DAG requires `dags_folder` to point at `/workspace/dags`. If the DAG
does not appear in the UI, verify this setting:

```bash
grep "dags_folder" ~/airflow/airflow.cfg
```

It must show:
dags_folder = /workspace/dags

If not, update it:

```bash
sed -i 's|dags_folder = /home/vscode/airflow/dags|dags_folder = /workspace/dags|' ~/airflow/airflow.cfg
```

### Environment Variables

The DAG requires `GCP_PROJECT_ID` to be available to Airflow worker
processes. Set it in the Airflow environment file:

```bash
echo "GCP_PROJECT_ID=your-project-id" >> ~/airflow/.env
echo "GOOGLE_CLOUD_PROJECT=your-project-id" >> ~/airflow/.env
```

Restart Airflow after making this change.

### Triggering the DAG

1. Open http://localhost:8080
2. Search for `donor_pipeline`
3. Click the toggle to unpause it
4. Click the play button and select "Trigger DAG w/ config"
5. Set the params:

```json
{
    "csv_path": "data/fec_sample.csv"
}
```

6. Set a logical date that has not been used before (e.g. today's date)
7. Click Trigger

For a full cycle ingestion use:

```json
{
    "csv_path": "data/itcont_2024.txt"
}
```

Note: full cycle ingestion takes 20-40 minutes per partition.

### Confirming a Successful Run

All 8 tasks should show green in the task graph. Verify the audit trail:

```sql
SELECT run_id, task_name, status, row_count_output, timestamp
FROM `your-project-id.metadata.pipeline_run_log`
ORDER BY timestamp DESC
LIMIT 8
```

All 8 rows should show status = PASS.

### Schedule

The DAG is scheduled monthly (`@monthly`) to match the FEC filing cadence.
For the demo, trigger manually using "Trigger DAG w/ config".

### Fail-Fast Behavior

To verify fail-fast behavior during development, temporarily raise an
exception in any check function in `pipelines/utils/pipeline_checks.py`.
Trigger the DAG and confirm all downstream tasks show as upstream-failed
in the UI. Revert the change before the demo.

### Troubleshooting

**DAG not appearing in UI**

Verify `dags_folder` points to `/workspace/dags` and check for import errors:

```bash
airflow dags list-import-errors
```

**Tasks failing with GCP auth errors**

Verify environment variables are set in `~/airflow/.env` and restart Airflow.
Also verify ADC credentials are fresh:

```bash
gcloud auth application-default login
```

**Duplicate logical date error when triggering**

Each logical date can only be used once per DAG. Use a new date for each
manual test run.


---

## Pre-Demo Checklist

Run through this checklist before every demo or presentation.

- [ ] Verify ADC credentials are fresh:
```bash
  gcloud auth application-default login
```
- [ ] Confirm environment variables are set:
```bash
  echo $GCP_PROJECT_ID
```
  If empty, export them before starting Airflow:
```bash
  export GCP_PROJECT_ID=project-a10238bd-a355-474b-b6a
  export GOOGLE_CLOUD_PROJECT=project-a10238bd-a355-474b-b6a
```
- [ ] Confirm dags_folder points to /workspace/dags:
```bash
  grep "dags_folder" ~/airflow/airflow.cfg
```
- [ ] Confirm GCP_PROJECT_ID is in Airflow env file:
```bash
  cat ~/airflow/.env
```
- [ ] Run full test suite:
```bash
  uv run pytest -v
```
- [ ] Start Airflow:
```bash
  airflow standalone
```
- [ ] Trigger DAG manually in UI against 50k sample partition
- [ ] Confirm all 8 tasks show green in task graph
- [ ] Confirm pipeline_run_log has 8 PASS entries
- [ ] Run lapsed donor query in BigQuery console and confirm results

### Recommended Demo Sequence

1. Show pre-loaded results in BigQuery (28M rows already processed)
2. Trigger the DAG live against the 50k sample (fast, safe, shows orchestration)
3. Show Airflow UI with all 8 tasks green
4. Query mart_donor_summary for lapsed major donors
5. Show pipeline_run_log audit trail

### Lapsed Donor Demo Query

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
