-- Frequency Distribution
--
-- Counts total contributions per donor across all time.
-- High frequency = loyal recurring donor.
-- Low frequency = one-time or occasional donor.

WITH donor_frequency AS (
    SELECT
        d.donor_id,
        COUNT(*) AS contribution_count
    FROM `project-a10238bd-a355-474b-b6a.core.dim_donors` d
    JOIN `project-a10238bd-a355-474b-b6a.staging.stg_contributions` s
        ON d.sub_id = s.sub_id
    GROUP BY d.donor_id
)

SELECT
    COUNT(*)                                            AS total_donors,
    MIN(contribution_count)                             AS min_contributions,
    MAX(contribution_count)                             AS max_contributions,
    ROUND(AVG(contribution_count), 2)                   AS avg_contributions,
    APPROX_QUANTILES(contribution_count, 100)[OFFSET(50)]   AS p50_contributions,
    APPROX_QUANTILES(contribution_count, 100)[OFFSET(75)]   AS p75_contributions,
    APPROX_QUANTILES(contribution_count, 100)[OFFSET(90)]   AS p90_contributions,
    COUNTIF(contribution_count = 1)                     AS donors_1_contribution,
    COUNTIF(contribution_count BETWEEN 2 AND 4)         AS donors_2_4_contributions,
    COUNTIF(contribution_count >= 5)                    AS donors_5_plus_contributions
FROM donor_frequency
