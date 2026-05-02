"""
BigQuery schema definition for raw.fec_contributions
Ticket 2.1 — Raw Table Schema Definition

This schema reflects the REAL FEC individual contributions file structure
as confirmed during data exploration (Ticket 1.1).

Column names are the ACTUAL FEC field names — not renamed or aliased.
Renaming happens in the staging layer, not here.

The raw layer is a receipt drawer:
store data exactly as it came in, with no transformation.

Reference:
    FEC data dictionary:
    https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/

    Column names sourced from:
    data/indiv_header_file.csv (committed to repo)
"""

RAW_FEC_SCHEMA = [
    # --- Committee receiving the contribution ---
    {"name": "CMTE_ID",          "type": "STRING",  "mode": "NULLABLE"},

    # --- Amendment indicator (N=new, A=amendment, T=termination) ---
    {"name": "AMNDT_IND",        "type": "STRING",  "mode": "NULLABLE"},

    # --- Report type ---
    {"name": "RPT_TP",           "type": "STRING",  "mode": "NULLABLE"},

    # --- Primary/general indicator ---
    {"name": "TRANSACTION_PGI",  "type": "STRING",  "mode": "NULLABLE"},

    # --- Image number (FEC filing image reference) ---
    {"name": "IMAGE_NUM",        "type": "STRING",  "mode": "NULLABLE"},

    # --- Transaction type code ---
    {"name": "TRANSACTION_TP",   "type": "STRING",  "mode": "NULLABLE"},

    # --- Entity type (IND=individual) ---
    {"name": "ENTITY_TP",        "type": "STRING",  "mode": "NULLABLE"},

    # --- Donor full name (Last, First format) ---
    {"name": "NAME",             "type": "STRING",  "mode": "NULLABLE"},

    # --- Donor city ---
    {"name": "CITY",             "type": "STRING",  "mode": "NULLABLE"},

    # --- Donor state (2-char abbreviation) ---
    {"name": "STATE",            "type": "STRING",  "mode": "NULLABLE"},

    # --- Donor ZIP code (may include ZIP+4 extension) ---
    {"name": "ZIP_CODE",         "type": "STRING",  "mode": "NULLABLE"},

    # --- Donor employer (1.7% null) ---
    {"name": "EMPLOYER",         "type": "STRING",  "mode": "NULLABLE"},

    # --- Donor occupation (1.7% null) ---
    {"name": "OCCUPATION",       "type": "STRING",  "mode": "NULLABLE"},

    # --- Contribution date (MMDDYYYY format — parsed in staging) ---
    {"name": "TRANSACTION_DT",   "type": "STRING",  "mode": "NULLABLE"},

    # --- Contribution amount in dollars ---
    {"name": "TRANSACTION_AMT",  "type": "NUMERIC", "mode": "NULLABLE"},

    # --- Other ID (37.2% null — not used in MVP) ---
    {"name": "OTHER_ID",         "type": "STRING",  "mode": "NULLABLE"},

    # --- Transaction ID (FEC assigned) ---
    {"name": "TRAN_ID",          "type": "STRING",  "mode": "NULLABLE"},

    # --- File number ---
    # FILE_NUM stored as STRING in raw — cast to INTEGER in staging if needed
    {"name": "FILE_NUM",         "type": "STRING",  "mode": "NULLABLE"},

    # --- Memo code (99.3% null — not used in MVP) ---
    {"name": "MEMO_CD",          "type": "STRING",  "mode": "NULLABLE"},

    # --- Memo text (48.8% null — not used in MVP) ---
    {"name": "MEMO_TEXT",        "type": "STRING",  "mode": "NULLABLE"},

    # --- Submission ID — 100% populated, primary MERGE key ---
    {"name": "SUB_ID",           "type": "STRING",  "mode": "NULLABLE"},

    # --- Pipeline load date — used for partition overwrite ---
    # REQUIRED: every row must have a load date for idempotent partitioning
    {"name": "_load_date",       "type": "DATE",    "mode": "REQUIRED"},
]
