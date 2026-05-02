-- Staging Table: stg_contributions
-- Ticket 3.2 — Staging SQL + Load Script
--
-- This table contains cleaned and normalized FEC contribution records.
-- Normalization is applied in Python (pipelines/utils/normalize.py)
-- before data reaches this table — NOT in SQL.
--
-- MERGE key: SUB_ID (confirmed 100% populated in data exploration)
-- Fallback key: donor_name_normalized + contribution_date + contribution_amount
--
-- Grain: one row per unique contribution (keyed on SUB_ID)

CREATE TABLE IF NOT EXISTS `donor_platform.staging.stg_contributions` (
    -- Source identifiers (preserved from raw)
    sub_id                  STRING,     -- Primary MERGE key (FEC submission ID)
    cmte_id                 STRING,     -- Committee receiving contribution
    tran_id                 STRING,     -- FEC transaction ID

    -- Raw donor fields (preserved for audit)
    name_raw                STRING,     -- Original NAME from FEC
    city_raw                STRING,     -- Original CITY from FEC
    state                   STRING,     -- STATE (2-char, minimal cleaning needed)
    zip_raw                 STRING,     -- Original ZIP_CODE from FEC

    -- Normalized fields (used for identity resolution)
    donor_name_normalized   STRING,     -- normalize_name(NAME)
    donor_address_normalized STRING,    -- normalize_address(CITY + STATE)
    zip_normalized          STRING,     -- normalize_zip(ZIP_CODE)

    -- Contribution details
    contribution_amount     NUMERIC,    -- TRANSACTION_AMT cast to NUMERIC
    contribution_date       DATE,       -- TRANSACTION_DT parsed from MMDDYYYY
    entity_type             STRING,     -- ENTITY_TP (filtered to IND in staging)

    -- Pipeline metadata
    _load_date              DATE        -- Partition key from raw layer
)
PARTITION BY _load_date;
