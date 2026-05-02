# Data Mart Definitions

## Engagement Score Thresholds

Based on actual data distributions analyzed from `dim_donors` and `stg_contributions` via `sql/marts/exploration/engagement_score_distribution_analysis.sql`, the following thresholds are defined for engagement score buckets:

### Recency (Days Since Last Donation)
| Score  | Threshold         |
|--------|-------------------|
| High   | 0–90 days         |
| Medium | 91–365 days       |
| Low    | 365+ days         |

> Justification: The 75th percentile (Q3) of days since last donation was observed at approximately 340 days. A clean split at 90/365 balances actionable recency while capturing long-tail inactivity.

### Frequency (Total Donations)
| Score  | Threshold         |
|--------|-------------------|
| High   | 5+ donations      |
| Medium | 2–4 donations     |
| Low    | 1 donation        |

> Justification: Median donation frequency is 1. The 75th percentile reaches 4 donations, making 5+ a meaningful high-engagement threshold.

### Monetary (Total Lifetime Donations)
| Score  | Threshold           |
|--------|---------------------|
| High   | $500+               |
| Medium | $100 – $499         |
| Low    | <$100               |

> Justification: The median total donated is $120, with Q3 at $480. Rounding to $500 creates a clear high-value donor category.

These thresholds are data-informed, operationally meaningful, and designed to support segmentation for outreach prioritization.