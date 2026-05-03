-- Core Table: dim_donors
-- Ticket 4.2 — Identity Resolution Layer
--
-- One row per canonical donor entity.
-- donor_id is deterministic — hash of the canonical matching key.
--
-- Matching rules (applied in order):
--   Rule 1: donor_name_normalized + zip_normalized
--   Rule 2: donor_name_normalized + donor_address_normalized
--   No match: hash of donor_name_normalized + sub_id (unique per record)
--
-- identity_conflict = TRUE when multiple distinct records
-- share the same matching key and cannot be resolved.
-- Conflicted records are ALSO written to dim_donors_unresolved.

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
