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
