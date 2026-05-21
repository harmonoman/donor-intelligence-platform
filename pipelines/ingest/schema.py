"""
BigQuery schema definition for raw.fec_contributions
Raw Table Schema Definition

This schema reflects the REAL FEC individual contributions file structure
as confirmed during data exploration.

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
    {"name": "CMTE_ID",          "type": "STRING",  "mode": "NULLABLE"},

    # Amendment indicator (N=new, A=amendment, T=termination)
    {"name": "AMNDT_IND",        "type": "STRING",  "mode": "NULLABLE"},

    {"name": "RPT_TP",           "type": "STRING",  "mode": "NULLABLE"},

    {"name": "TRANSACTION_PGI",  "type": "STRING",  "mode": "NULLABLE"},

    {"name": "IMAGE_NUM",        "type": "STRING",  "mode": "NULLABLE"},

    {"name": "TRANSACTION_TP",   "type": "STRING",  "mode": "NULLABLE"},

    # Entity type (IND=individual)
    {"name": "ENTITY_TP",        "type": "STRING",  "mode": "NULLABLE"},

    {"name": "NAME",             "type": "STRING",  "mode": "NULLABLE"},

    {"name": "CITY",             "type": "STRING",  "mode": "NULLABLE"},

    {"name": "STATE",            "type": "STRING",  "mode": "NULLABLE"},

    {"name": "ZIP_CODE",         "type": "STRING",  "mode": "NULLABLE"},

    {"name": "EMPLOYER",         "type": "STRING",  "mode": "NULLABLE"},

    {"name": "OCCUPATION",       "type": "STRING",  "mode": "NULLABLE"},

    # Contribution date (MMDDYYYY format — parsed in staging)
    {"name": "TRANSACTION_DT",   "type": "STRING",  "mode": "NULLABLE"},

    {"name": "TRANSACTION_AMT",  "type": "NUMERIC", "mode": "NULLABLE"},

    # Other ID (37.2% null — not used in MVP)
    {"name": "OTHER_ID",         "type": "STRING",  "mode": "NULLABLE"},

    {"name": "TRAN_ID",          "type": "STRING",  "mode": "NULLABLE"},

    # FILE_NUM stored as STRING in raw — cast to INTEGER in staging if needed
    {"name": "FILE_NUM",         "type": "STRING",  "mode": "NULLABLE"},

    # Memo code (99.3% null — not used in MVP)
    {"name": "MEMO_CD",          "type": "STRING",  "mode": "NULLABLE"},

    # Memo text (48.8% null — not used in MVP)
    {"name": "MEMO_TEXT",        "type": "STRING",  "mode": "NULLABLE"},

    # Submission ID — 100% populated, primary MERGE key
    {"name": "SUB_ID",           "type": "STRING",  "mode": "NULLABLE"},

    # Pipeline load date — used for partition overwrite
    # REQUIRED: every row must have a load date for idempotent partitioning
    {"name": "_load_date",       "type": "DATE",    "mode": "REQUIRED"},
]
