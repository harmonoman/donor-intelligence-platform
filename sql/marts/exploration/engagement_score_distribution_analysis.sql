WITH donation_stats AS (
    SELECT
        d.donor_id,
        MAX(c.donation_date) AS last_donation_date,
        COUNT(c.contribution_id) AS donation_frequency,
        SUM(c.donation_amount) AS total_donated
    FROM
        dim_donors d
    LEFT JOIN
        stg_contributions c ON d.donor_id = c.donor_id
    GROUP BY
        d.donor_id
),
recency_analysis AS (
    SELECT
        EXTRACT(DAY FROM (CURRENT_DATE - last_donation_date)) AS days_since_last_donation
    FROM
        donation_stats
    WHERE
        last_donation_date IS NOT NULL
),
frequency_analysis AS (
    SELECT
        donation_frequency
    FROM
        donation_stats
),
monetary_analysis AS (
    SELECT
        total_donated
    FROM
        donation_stats
    WHERE
        total_donated IS NOT NULL
)
SELECT
    -- Recency: percentiles for days since last donation
    percentile_disc(0.25) WITHIN GROUP (ORDER BY days_since_last_donation) AS recency_q1,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY days_since_last_donation) AS recency_median,
    percentile_disc(0.75) WITHIN GROUP (ORDER BY days_since_last_donation) AS recency_q3,
    MIN(days_since_last_donation) AS recency_min,
    MAX(days_since_last_donation) AS recency_max,

    -- Frequency: distribution summary
    percentile_disc(0.25) WITHIN GROUP (ORDER BY donation_frequency) AS frequency_q1,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY donation_frequency) AS frequency_median,
    percentile_disc(0.75) WITHIN GROUP (ORDER BY donation_frequency) AS frequency_q3,
    MIN(donation_frequency) AS frequency_min,
    MAX(donation_frequency) AS frequency_max,

    -- Monetary: total donated distribution
    percentile_disc(0.25) WITHIN GROUP (ORDER BY total_donated) AS monetary_q1,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY total_donated) AS monetary_median,
    percentile_disc(0.75) WITHIN GROUP (ORDER BY total_donated) AS monetary_q3,
    MIN(total_donated) AS monetary_min,
    MAX(total_donated) AS monetary_max
FROM
    recency_analysis
CROSS JOIN
    frequency_analysis
CROSS JOIN
    monetary_analysis;