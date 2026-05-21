-- Recency Distribution
--
-- Measures how recently each donor last contributed.
-- Reference date: max contribution_date in dataset (not current_date)
-- because the FEC sample is historical, not live.
--
-- Lower days_since_last = more recent = higher engagement

WITH donor_last_contribution AS (
    SELECT
        d.donor_id,
        MAX(s.contribution_date) AS last_contribution_date
    FROM `project-a10238bd-a355-474b-b6a.core.dim_donors` d
    JOIN `project-a10238bd-a355-474b-b6a.staging.stg_contributions` s
        ON d.sub_id = s.sub_id
    WHERE s.contribution_date IS NOT NULL
    GROUP BY d.donor_id
),

reference_date AS (
    SELECT MAX(last_contribution_date) AS ref_date
    FROM donor_last_contribution
),

recency AS (
    SELECT
        d.donor_id,
        DATE_DIFF(r.ref_date, d.last_contribution_date, DAY) AS days_since_last
    FROM donor_last_contribution d
    CROSS JOIN reference_date r
)

SELECT
    COUNT(*)                                        AS total_donors,
    MIN(days_since_last)                            AS min_days,
    MAX(days_since_last)                            AS max_days,
    ROUND(AVG(days_since_last), 1)                  AS avg_days,
    APPROX_QUANTILES(days_since_last, 100)[OFFSET(25)]  AS p25_days,
    APPROX_QUANTILES(days_since_last, 100)[OFFSET(50)]  AS p50_days,
    APPROX_QUANTILES(days_since_last, 100)[OFFSET(75)]  AS p75_days,
    APPROX_QUANTILES(days_since_last, 100)[OFFSET(90)]  AS p90_days,
    COUNTIF(days_since_last <= 90)                  AS donors_0_90_days,
    COUNTIF(days_since_last BETWEEN 91 AND 365)     AS donors_91_365_days,
    COUNTIF(days_since_last > 365)                  AS donors_over_365_days
FROM recency
