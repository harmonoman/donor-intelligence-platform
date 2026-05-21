-- Monetary Distribution
--
-- Total contribution amount per donor across all time.
-- Uses NUMERIC type — no floating point precision issues.
-- High monetary = high-value donor.

WITH donor_monetary AS (
    SELECT
        d.donor_id,
        SUM(s.contribution_amount) AS total_amount
    FROM `project-a10238bd-a355-474b-b6a.core.dim_donors` d
    JOIN `project-a10238bd-a355-474b-b6a.staging.stg_contributions` s
        ON d.sub_id = s.sub_id
    WHERE s.contribution_amount IS NOT NULL
    GROUP BY d.donor_id
)

SELECT
    COUNT(*)                                            AS total_donors,
    MIN(total_amount)                                   AS min_amount,
    MAX(total_amount)                                   AS max_amount,
    ROUND(AVG(total_amount), 2)                         AS avg_amount,
    APPROX_QUANTILES(total_amount, 100)[OFFSET(25)]     AS p25_amount,
    APPROX_QUANTILES(total_amount, 100)[OFFSET(50)]     AS p50_amount,
    APPROX_QUANTILES(total_amount, 100)[OFFSET(75)]     AS p75_amount,
    APPROX_QUANTILES(total_amount, 100)[OFFSET(90)]     AS p90_amount,
    COUNTIF(total_amount < 100)                         AS donors_under_100,
    COUNTIF(total_amount BETWEEN 100 AND 499)           AS donors_100_499,
    COUNTIF(total_amount >= 500)                        AS donors_500_plus
FROM donor_monetary
