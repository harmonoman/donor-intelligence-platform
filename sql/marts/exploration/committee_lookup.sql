-- Committee Name Lookup
-- Ticket 5.1 — Exploration
--
-- Surfaces committee IDs and contribution counts from staging.
-- FEC does not provide committee names in the individual contributions file.
-- This query identifies the top committees by contribution volume
-- so we can manually map names for the mart if needed,
-- or document that CMTE_ID enrichment is a post-MVP enhancement.
--
-- Note: Full committee name enrichment requires joining to the
-- FEC committee master file (cm.txt) — tracked as post-MVP.

SELECT
    s.cmte_id,
    COUNT(*)                        AS contribution_count,
    COUNT(DISTINCT d.donor_id)      AS unique_donors,
    SUM(s.contribution_amount)      AS total_amount
FROM `project-a10238bd-a355-474b-b6a.staging.stg_contributions` s
JOIN `project-a10238bd-a355-474b-b6a.core.dim_donors` d
    ON s.sub_id = d.sub_id
WHERE s.cmte_id IS NOT NULL
GROUP BY s.cmte_id
ORDER BY contribution_count DESC
LIMIT 20
