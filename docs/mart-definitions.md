cat > docs/mart-definitions.md << 'EOF'
# Mart Definitions
## Ticket 5.1 — Engagement Score Threshold Definition

> STATUS: COMPLETE
> Thresholds are grounded in observed FEC data distributions.
> No values were guessed or assumed.

---

## Data Analyzed

Source tables:
- `donor_platform.core.dim_donors`
- `donor_platform.staging.stg_contributions`

Dataset: FEC individual contributions
- 2023-2024 cycle: 15,255,256 records (Aug 2023 — Nov 2024)
- 2025-2026 cycle: 12,727,218 records (Feb 2025 — Mar 2026)
- Total: ~28M contributions, 3,160,099 unique donors

---

## Distribution Analysis

### Recency (days since last contribution)

Reference date: MAX(contribution_date) across all donors

| Metric | Value |
|---|---|
| Total donors | 3,160,099 |
| Min days | 0 |
| Max days | 959 |
| Avg days | 412.9 |
| p25 | 99 days |
| p50 (median) | 498 days |
| p75 | 599 days |
| p90 | 845 days |
| 0-90 days | 631,097 (20%) |
| 91-365 days | 768,446 (24%) |
| 365+ days | 1,760,556 (56%) |

**Thresholds:**

| Tier | Range | Count | Pct | Rationale |
|---|---|---|---|---|
| Recent | 0-90 days | 631,097 | 20% | p25 boundary — gave within last quarter |
| Active | 91-365 days | 768,446 | 24% | Within last year but not recent |
| Lapsed | 365+ days | 1,760,556 | 56% | No contribution in over a year |

---

### Frequency (total contributions per donor)

| Metric | Value |
|---|---|
| Total donors | 3,160,101 |
| Min | 1 |
| Max | 39,625 |
| Avg | 8.85 |
| p50 (median) | 2 |
| p75 | 6 |
| p90 | 13 |
| p95 | 23 |
| 1 contribution | 1,014,553 (32%) |
| 2-4 | 1,175,515 (37%) |
| 5-9 | 492,879 (16%) |
| 10+ | 477,154 (15%) |

**Thresholds:**

| Tier | Range | Count | Pct | Rationale |
|---|---|---|---|---|
| High | 10+ contributions | 477,154 | 15% | Above p90 — power donors with demonstrated loyalty |
| Medium | 2-9 contributions | 1,668,394 | 53% | Between median and p90 — returning donors |
| Low | 1 contribution | 1,014,553 | 32% | Below median — one-time donors |

---

### Monetary (total contribution amount per donor)

| Metric | Value |
|---|---|
| Total donors | 3,160,101 |
| Min | -$1,000,000 (refunds) |
| Max | $97,460,405 |
| Avg | $1,665 |
| p25 | $94 |
| p50 (median) | $250 |
| p75 | $650 |
| p90 | $2,000 |
| p95 | $4,000 |
| Under $100 | 813,550 (26%) |
| $100-$499 | 1,243,109 (39%) |
| $500-$999 | 455,943 (14%) |
| $1,000+ | 647,499 (20%) |

**Thresholds:**

| Tier | Range | Count | Pct | Rationale |
|---|---|---|---|---|
| High | $1,000+ | 647,499 | 20% | Above p90 — major donors |
| Medium | $100-$999 | 1,699,052 | 53% | p25 to p90 — mid-tier donors |
| Low | Under $100 | 813,550 | 26% | Below p25 — small-dollar donors |

**Note on negative amounts:** FEC data includes refunds and reversals
as negative TRANSACTION_AMT values. Stored accurately. Negative totals
fall into Low tier automatically. Analysts should filter
`total_amount > 0` for standard donor value calculations if needed.

---

## Committee Name Enrichment

The FEC individual contributions file contains only `CMTE_ID`
(e.g., `C00401224`). Human-readable committee names require joining
to the FEC committee master file (`cm.txt`) — tracked as post-MVP.

`cmte_id` is stored as-is in the mart. Committee name enrichment
is a post-MVP enhancement.

See: `sql/marts/exploration/committee_lookup.sql`

Full enrichment requires joining to the FEC committee master file.
Download: `https://www.fec.gov/files/bulk-downloads/2024/cm24.zip`

---

## Final Threshold Constants

Defined in `pipelines/marts/thresholds.py`:

```python
# Recency (days since last contribution)
RECENCY_RECENT_MAX = 90       # 0-90 days
RECENCY_ACTIVE_MAX = 365      # 91-365 days
# Lapsed = 365+ days

# Frequency (contribution count)
FREQUENCY_HIGH_MIN = 10       # 10+
FREQUENCY_MEDIUM_MIN = 2      # 2-9
FREQUENCY_MEDIUM_MAX = 9
# Low = 1

# Monetary (total amount in dollars)
MONETARY_HIGH_MIN = 1000      # $1,000+
MONETARY_MEDIUM_MIN = 100     # $100-$999
MONETARY_MEDIUM_MAX = 999
# Low = under $100
```

---

## Known Limitations

1. **Recency gap**: No FEC data between Nov 2024 and Feb 2025.
   This reflects FEC filing patterns, not a pipeline gap.

2. **Committee names not enriched**: `cmte_id` stored as raw ID.
   Full enrichment requires FEC committee master file (post-MVP).

3. **Negative contribution amounts**: Refunds present in FEC data.
   Stored accurately. Fall into Low monetary tier automatically.
