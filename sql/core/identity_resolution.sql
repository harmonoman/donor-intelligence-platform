-- Identity Resolution Query
-- Assigns stable donor_ids to normalized staging records.
--
-- Two-pass matching logic:
--   Pass 1 (Rule 1): match on donor_name_normalized + zip_normalized
--   Pass 2 (Rule 2): for records with no ZIP, match on name + address
--
-- Key principle:
--   Same canonical key = same donor. Always.
--   donor_id = MD5(canonical_key) — deterministic and reproducible.
--   identity_conflict is always FALSE in this implementation.
--   True collision detection requires incremental matching — deferred to post-MVP.
--
-- Placeholders replaced at runtime:
--   {PROJECT_ID}      — GCP project ID
--   {SOURCE_TABLE}    — staging table to resolve (e.g. staging.stg_contributions)
--   {EXECUTION_DATE}  — partition date (YYYY-MM-DD)

WITH source AS (
    SELECT
        sub_id,
        donor_name_normalized,
        donor_address_normalized,
        zip_normalized,
        CAST(_load_date AS STRING) AS _load_date
    FROM `{PROJECT_ID}.{SOURCE_TABLE}`
    WHERE CAST(_load_date AS STRING) = '{EXECUTION_DATE}'
),

-- Pass 1: Rule 1 — match on name + ZIP
rule1_matches AS (
    SELECT
        sub_id,
        donor_name_normalized,
        donor_address_normalized,
        zip_normalized,
        _load_date,
        CONCAT(donor_name_normalized, '|zip:', zip_normalized) AS canonical_key,
        'rule1' AS match_rule
    FROM source
    WHERE zip_normalized IS NOT NULL AND zip_normalized != ''
),

-- Pass 2: Rule 2 candidates — records with no ZIP
rule2_candidates AS (
    SELECT
        sub_id,
        donor_name_normalized,
        donor_address_normalized,
        zip_normalized,
        _load_date
    FROM source
    WHERE zip_normalized IS NULL OR zip_normalized = ''
),

-- Pass 2: Rule 2 — look up whether a Rule 1 record shares name + address
rule2_matches AS (
    SELECT
        r2.sub_id,
        r2.donor_name_normalized,
        r2.donor_address_normalized,
        r2.zip_normalized,
        r2._load_date,
        COALESCE(
            MIN(r1.canonical_key),
            CASE
                WHEN r2.donor_address_normalized IS NOT NULL
                     AND r2.donor_address_normalized != ''
                    THEN CONCAT(r2.donor_name_normalized, '|addr:', r2.donor_address_normalized)
                ELSE CONCAT(r2.donor_name_normalized, '|id:', r2.sub_id)
            END
        ) AS canonical_key,
        CASE
            WHEN MIN(r1.canonical_key) IS NOT NULL THEN 'rule2'
            WHEN r2.donor_address_normalized IS NOT NULL
                 AND r2.donor_address_normalized != '' THEN 'rule2'
            ELSE 'no_match'
        END AS match_rule
    FROM rule2_candidates r2
    LEFT JOIN rule1_matches r1
        ON r2.donor_name_normalized = r1.donor_name_normalized
        AND r2.donor_address_normalized = r1.donor_address_normalized
        AND r1.donor_address_normalized IS NOT NULL
        AND r1.donor_address_normalized != ''
    GROUP BY
        r2.sub_id, r2.donor_name_normalized, r2.donor_address_normalized,
        r2.zip_normalized, r2._load_date
),

-- Combine both passes
keyed AS (
    SELECT * FROM rule1_matches
    UNION ALL
    SELECT * FROM rule2_matches
),

-- Generate deterministic donor_id from canonical_key
with_donor_id AS (
    SELECT
        *,
        TO_HEX(MD5(canonical_key)) AS donor_id
    FROM keyed
),

-- All records sharing a canonical key resolve to the same donor_id
-- identity_conflict is always FALSE — same key always means same donor
final AS (
    SELECT
        w.donor_id,
        w.sub_id,
        w.donor_name_normalized,
        w.donor_address_normalized,
        w.zip_normalized,
        w.match_rule,
        CAST(w._load_date AS STRING) AS _load_date,
        FALSE AS identity_conflict
    FROM with_donor_id w
)

SELECT * FROM final
