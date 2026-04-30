# GCP Setup Guide
## Donor Intelligence Platform — Ticket 1.2

> ⚠️ STATUS: COMPLETE \
> Last updated: April 29, 2025 \
> Author: Mark

This guide documents how to configure Google Cloud Platform and BigQuery
for local development on the Donor Intelligence Platform.

Authentication uses **Application Default Credentials (ADC)** — no service
account key file required.

---

## Prerequisites

- A Google account
- Docker + VS Code Dev Container running
- `gcloud` CLI installed (see Step 2)

---

## Step 1 — Create a GCP Account + Project

1. Go to https://console.cloud.google.com
2. Sign in with your Google account
3. Accept the free trial ($300 credits, 90 days) if prompted
4. A default project will be created automatically
5. Note your **Project ID** — you will need it in Step 4

> This project uses Project ID: `project-a10238bd-a355-474b-b6a` \
> Use your own project ID when setting up a fresh environment.

---

## Step 2 — Install gcloud CLI

Run inside the dev container (use `bash` shell, not `sh`):

```bash
bash
curl https://sdk.cloud.google.com | bash
```

When prompted:

- Opt out of usage reporting: `n`
- Update rc file path: press Enter to accept default (`/home/vscode/.bashrc`)
- Continue: `Y`

Reload shell and verify:

```bash
source ~/.bashrc
gcloud --version
```

Expected output: \
Google Cloud SDK 566.0.0 \
bq 2.1.31 \
...

---

## Step 3 — Initialize gcloud

```bash
gcloud init
```

When prompted:

- Sign in: `Y` — follow the browser URL to authenticate
- Pick project: select your project from the list

---

## Step 4 — Configure Application Default Credentials (ADC)

```bash
gcloud auth application-default login
```

Follow the browser URL, sign in with the same Google account.

Credentials will be saved to: \
`/home/vscode/.config/gcloud/application_default_credentials.json`

This file is used automatically by the BigQuery Python client.
It does not need to be referenced in `.env` or committed to the repo.

---

## Step 5 — Configure `.env`

Copy the example file and populate it:

```bash
cp .env.example .env
```

Edit `.env`:

```bash
GCP_PROJECT_ID=your-project-id
# GOOGLE_APPLICATION_CREDENTIALS is not needed — ADC handles auth automatically
```

Confirm `.env` is gitignored:

```bash
git check-ignore -v .env
```

Expected output: \
`.gitignore:X:    .env    .env`

---

## Step 6 — Enable BigQuery API

1. Go to https://console.cloud.google.com
2. Sidebar → **APIs & Services** → **Enable APIs and Services**
3. Search `BigQuery API` → **Enable**

---

## Step 7 — Create BigQuery Datasets

Run the setup script:

```bash
uv run python scripts/setup_bigquery.py
```

Expected output: \
✅ Created  : your-project-id.raw \
✅ Created  : your-project-id.staging \
✅ Created  : your-project-id.core \
✅ Created  : your-project-id.marts \
✅ Created  : your-project-id.metadata \
✅ BigQuery connection verified 

The script is idempotent — safe to run multiple times.

---

## Step 8 — Verify With Tests

```bash
uv run pytest tests/integration/test_bigquery_connection.py -v
```

Expected output: \
test_bigquery_client_connects     PASSED \
test_bigquery_query_executes      PASSED \
test_required_datasets_exist      PASSED \
test_dataset_locations            PASSED \
4 passed

---

## Notes on Authentication

This project uses **Application Default Credentials (ADC)** instead of a
service account JSON key file.

**Why:** GCP free trial accounts enforce an org policy
(`iam.disableServiceAccountKeyCreation`) that blocks JSON key creation.
ADC is the more secure modern alternative and requires no key management.

**How it works:** After running `gcloud auth application-default login`,
the Google auth library automatically finds and uses the credentials file
at `/home/vscode/.config/gcloud/application_default_credentials.json`.
No configuration in `.env` or code is required.

**Important:** ADC credentials are tied to your local dev container
environment. They are not committed to the repo. Every engineer setting
up a fresh environment must run Steps 2–4 to authenticate.

---

## BigQuery Dataset Reference

| Dataset | Full ID | Purpose |
|---|---|---|
| raw | `your-project-id.raw` | Unmodified source data |
| staging | `your-project-id.staging` | Cleaned and normalized records |
| core | `your-project-id.core` | Identity resolution output |
| marts | `your-project-id.marts` | Analytics-ready tables |
| metadata | `your-project-id.metadata` | Pipeline run logs |

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'pipelines'`** 
- Confirm `pyproject.toml` has `pythonpath = ["."]` under
`[tool.pytest.ini_options]`.

**`GCP_PROJECT_ID is not set`** 
- Confirm `.env` exists and contains `GCP_PROJECT_ID=your-project-id`.

**`DefaultCredentialsError`** 
- Re-run `gcloud auth application-default login` — ADC credentials
may have expired or the `GOOGLE_APPLICATION_CREDENTIALS` env var
may be pointing at a file that doesn't exist. Comment it out in `.env`.

**ADC Credential Refresh** 
- ADC credentials expire periodically. If BigQuery connections fail
after a break of several days, refresh credentials:
    ```bash
    gcloud auth application-default login
    ```
- Then rerun tests to confirm connectivity is restored.

**`GOOGLE_APPLICATION_CREDENTIALS` pointing at missing file**
- Comment out or remove `GOOGLE_APPLICATION_CREDENTIALS` from `.env`.
ADC does not need it.

**`gcloud: command not found` after container restart**
- The gcloud SDK is installed in a non-standard path due to the install
method used. Add it to your PATH manually:
    ```bash
    export PATH="/workspace/exec -l /bin/sh/google-cloud-sdk/bin:$PATH"
    ```
- To make this permanent, add the above line to `~/.bashrc`.
