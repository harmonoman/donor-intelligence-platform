-- Mart: mart_donor_summary

--
-- Grain: one row per donor_id
-- Source: core.dim_donors joined to staging.stg_contributions
--
-- Engagement scoring thresholds defined in pipelines/marts/thresholds.py:
--   Recency:   recent <= 90 days, active <= 365 days, lapsed > 365 days
--   Frequency: high >= 10, medium >= 2, low = 1
--   Monetary:  high >= $1000, medium >= $100, low < $100
--
-- Engagement score formula:
--   engagement_score = 0.4 * recency_score + 0.3 * frequency_score + 0.3 * monetary_score
--
-- Reference date for recency: MAX(last_donation_date) across all donors.
-- Using dataset max rather than CURRENT_DATE() because FEC data is historical.
-- This ensures recency reflects data reality, not calendar drift.
--
-- Full rebuild on every run. No incremental logic.
-- Idempotency guaranteed by WRITE_TRUNCATE on target table.
--
-- Note: contribution_date IS NOT NULL filter excludes donors whose
-- contributions all have unparseable TRANSACTION_DT values in the
-- FEC source file. Confirmed: 2 donors excluded as of 2024 cycle.
-- These donors exist in dim_donors but have no valid contribution dates.

CREATE OR REPLACE TABLE `{PROJECT_ID}.marts.mart_donor_summary` AS

WITH contributions AS (
    SELECT
        d.donor_id,
        d.donor_name_normalized,
        d.donor_address_normalized,
        d.zip_normalized,
        d.match_rule,
        s.contribution_amount,
        s.contribution_date,
        s.cmte_id
    FROM `{PROJECT_ID}.core.dim_donors` d
    JOIN `{PROJECT_ID}.staging.stg_contributions` s
        ON d.sub_id = s.sub_id
    WHERE s.contribution_date IS NOT NULL
),

donor_metrics AS (
    SELECT
        donor_id,
        MAX(donor_name_normalized)      AS donor_name_normalized,
        MAX(donor_address_normalized)   AS donor_address_normalized,
        MAX(zip_normalized)             AS zip_normalized,
        MIN(match_rule)                 AS match_rule,
        COUNT(*)                        AS contribution_count,
        SUM(contribution_amount)        AS total_contributions,
        MIN(contribution_date)          AS first_donation_date,
        MAX(contribution_date)          AS last_donation_date,
        -- Committee donations: comma-separated list of unique cmte_ids
        STRING_AGG(DISTINCT cmte_id ORDER BY cmte_id LIMIT 50)  AS committee_ids
    FROM contributions
    GROUP BY donor_id
),

reference_date AS (
    SELECT MAX(last_donation_date) AS ref_date
    FROM donor_metrics
),

scored AS (
    SELECT
        m.donor_id,
        m.donor_name_normalized,
        m.donor_address_normalized,
        m.zip_normalized,
        m.match_rule,
        m.contribution_count,
        m.total_contributions,
        m.first_donation_date,
        m.last_donation_date,
        m.committee_ids,
        DATE_DIFF(r.ref_date, m.last_donation_date, DAY)   AS days_since_last_donation,

        -- Recency score (0-90=3, 91-365=2, 365+=1)
        CASE
            WHEN DATE_DIFF(r.ref_date, m.last_donation_date, DAY) <= 90  THEN 3.0
            WHEN DATE_DIFF(r.ref_date, m.last_donation_date, DAY) <= 365 THEN 2.0
            ELSE 1.0
        END AS recency_score,

        -- Frequency score (10+=3, 2-9=2, 1=1)
        CASE
            WHEN m.contribution_count >= 10 THEN 3.0
            WHEN m.contribution_count >= 2  THEN 2.0
            ELSE 1.0
        END AS frequency_score,

        -- Monetary score ($1000+=3, $100-$999=2, <$100=1)
        CASE
            WHEN m.total_contributions >= 1000 THEN 3.0
            WHEN m.total_contributions >= 100  THEN 2.0
            ELSE 1.0
        END AS monetary_score

    FROM donor_metrics m
    CROSS JOIN reference_date r
)

SELECT
    donor_id,
    donor_name_normalized,
    donor_address_normalized,
    zip_normalized,
    match_rule,
    contribution_count,
    total_contributions,
    first_donation_date,
    last_donation_date,
    days_since_last_donation,
    committee_ids,
    recency_score,
    frequency_score,
    monetary_score,
    ROUND(0.4 * recency_score + 0.3 * frequency_score + 0.3 * monetary_score, 2) AS engagement_score
FROM scored
