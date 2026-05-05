-- Core Table: dim_donors
-- Tickets 4.2 + 4.3 — Identity Resolution Layer
--
-- NOTE: This file is documentation only — the table is created
-- programmatically via ensure_dim_donors_exists() in build_identity.py.
-- BigQuery REQUIRED mode is equivalent to NOT NULL shown here.
-- Keep this file in sync with the schema in build_identity.py.
--
-- One row per canonical donor entity.
-- donor_id is deterministic — hash of the canonical matching key.
--
-- Matching rules (applied in order):
--   Rule 1: donor_name_normalized + zip_normalized
--   Rule 2: donor_name_normalized + donor_address_normalized
--   No match: hash of donor_name_normalized + sub_id (unique per record)
--
-- identity_conflict is always FALSE in current implementation.
-- Same canonical key always resolves to same donor_id.
-- True collision detection deferred to post-MVP.
-- dim_donors_unresolved exists but is not populated in current implementation.

CREATE TABLE IF NOT EXISTS `donor_platform.core.dim_donors` (
    donor_id                 STRING    NOT NULL,  -- deterministic hash
    sub_id                   STRING,              -- source record identifier
    donor_name_normalized    STRING,              -- from staging
    donor_address_normalized STRING,              -- from staging
    zip_normalized           STRING,              -- from staging
    match_rule               STRING,              -- rule1 / rule2 / no_match
    identity_conflict        BOOL      DEFAULT FALSE,
    _load_date               DATE                 -- from staging
)
PARTITION BY _load_date;

CREATE TABLE IF NOT EXISTS `donor_platform.core.dim_donors_unresolved` (
    donor_id                 STRING,
    sub_id                   STRING,
    donor_name_normalized    STRING,
    donor_address_normalized STRING,
    zip_normalized           STRING,
    conflict_reason          STRING,
    _load_date               DATE
)
PARTITION BY _load_date;
