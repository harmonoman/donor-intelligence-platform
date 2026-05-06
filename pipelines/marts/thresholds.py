"""
Mart Engagement Score Thresholds
Ticket 5.1 — Engagement Score Threshold Definition

Thresholds grounded in observed distributions across 3,160,099 donors
from 28M FEC contributions (Aug 2023 — Mar 2026).
See docs/mart-definitions.md for full analysis.

Distribution summary:
    Recency:   p25=99d, p50=498d, p75=599d, p90=845d
    Frequency: p50=2, p75=6, p90=13, p95=23, max=39,625
    Monetary:  p25=$94, p50=$250, p75=$650, p90=$2,000, max=$97.4M
"""

# ---------------------------------------------------------------------------
# Recency thresholds (days since last contribution)
# ---------------------------------------------------------------------------

RECENCY_RECENT_MAX = 90       # 0-90 days   — 20% of donors (p25 boundary)
RECENCY_ACTIVE_MAX = 365      # 91-365 days — 24% of donors (1 year boundary)
# Lapsed = 365+ days — 56% of donors

# ---------------------------------------------------------------------------
# Frequency thresholds (total contribution count per donor)
# ---------------------------------------------------------------------------

FREQUENCY_HIGH_MIN = 10       # 10+ contributions — 15% (above p90 — power donors)
FREQUENCY_MEDIUM_MIN = 2      # 2-9 contributions  — 53% (p50 to p90)
FREQUENCY_MEDIUM_MAX = 9
# Low = 1 contribution — 32% of donors (below median)

# ---------------------------------------------------------------------------
# Monetary thresholds (total contribution amount per donor, in dollars)
# ---------------------------------------------------------------------------

MONETARY_HIGH_MIN = 1000      # $1,000+   — 20% (above p90 — major donors)
MONETARY_MEDIUM_MIN = 100     # $100-$999 — 53% (p25 to p90)
MONETARY_MEDIUM_MAX = 999
# Low = under $100 — 26% of donors (below p25)
# Note: negative amounts (refunds) fall into Low tier — correct behavior
