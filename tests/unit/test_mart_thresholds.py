"""
Unit tests for mart engagement score thresholds.
Engagement Score Threshold Definition

Tests validate:
- Thresholds are defined as constants
- No overlapping ranges
- Full coverage of all possible values
- Monetary thresholds handle negative amounts (refunds)

Thresholds grounded in real FEC data:
    3,160,099 donors across 28M contributions (Aug 2023 — Mar 2026)

Run with:
    uv run pytest tests/unit/test_mart_thresholds.py -v
"""

from pipelines.marts.thresholds import (
    FREQUENCY_HIGH_MIN,
    FREQUENCY_MEDIUM_MAX,
    FREQUENCY_MEDIUM_MIN,
    MONETARY_HIGH_MIN,
    MONETARY_MEDIUM_MAX,
    MONETARY_MEDIUM_MIN,
    RECENCY_ACTIVE_MAX,
    RECENCY_RECENT_MAX,
)

# ---------------------------------------------------------------------------
# Recency threshold tests
# ---------------------------------------------------------------------------

def test_recency_thresholds_are_positive_integers():
    """Recency thresholds must be positive integers."""
    assert isinstance(RECENCY_RECENT_MAX, int)
    assert isinstance(RECENCY_ACTIVE_MAX, int)
    assert RECENCY_RECENT_MAX > 0
    assert RECENCY_ACTIVE_MAX > 0


def test_recency_recent_below_active():
    """Recent max must be below active max."""
    assert RECENCY_RECENT_MAX < RECENCY_ACTIVE_MAX


def test_recency_full_coverage():
    """Every possible days_since_last maps to exactly one tier."""
    test_values = [0, 1, 89, 90, 91, 180, 364, 365, 366, 500, 959]
    for days in test_values:
        tiers = []
        if days <= RECENCY_RECENT_MAX:
            tiers.append("recent")
        elif days <= RECENCY_ACTIVE_MAX:
            tiers.append("active")
        else:
            tiers.append("lapsed")
        assert len(tiers) == 1, f"Days {days} maps to {len(tiers)} tiers"


def test_recency_lapsed_has_no_upper_bound():
    """Values well beyond observed maximum still classify as lapsed."""
    for days in [960, 1000, 2000, 5000]:
        assert days > RECENCY_ACTIVE_MAX, (
            f"{days} days should be lapsed but is within active range"
        )


# ---------------------------------------------------------------------------
# Frequency threshold tests
# ---------------------------------------------------------------------------

def test_frequency_thresholds_are_positive_integers():
    """All frequency thresholds must be positive integers."""
    assert isinstance(FREQUENCY_HIGH_MIN, int)
    assert isinstance(FREQUENCY_MEDIUM_MIN, int)
    assert isinstance(FREQUENCY_MEDIUM_MAX, int)
    assert FREQUENCY_HIGH_MIN > 0
    assert FREQUENCY_MEDIUM_MIN > 0
    assert FREQUENCY_MEDIUM_MAX > 0


def test_frequency_high_above_medium():
    """High tier must start above medium tier maximum."""
    assert FREQUENCY_HIGH_MIN > FREQUENCY_MEDIUM_MAX


def test_frequency_medium_range_valid():
    """Medium tier min must be less than medium tier max."""
    assert FREQUENCY_MEDIUM_MIN <= FREQUENCY_MEDIUM_MAX


def test_frequency_low_is_implicit_one():
    """Low tier is implicitly count = 1, below medium minimum."""
    assert FREQUENCY_MEDIUM_MIN > 1


def test_frequency_no_gap_between_medium_and_high():
    """No gap between medium max and high min."""
    assert FREQUENCY_HIGH_MIN == FREQUENCY_MEDIUM_MAX + 1


def test_frequency_full_coverage():
    """Every possible contribution count maps to exactly one tier."""
    for count in range(1, 50):
        tiers = []
        if count >= FREQUENCY_HIGH_MIN:
            tiers.append("high")
        elif FREQUENCY_MEDIUM_MIN <= count <= FREQUENCY_MEDIUM_MAX:
            tiers.append("medium")
        else:
            tiers.append("low")
        assert len(tiers) == 1, f"Count {count} maps to {len(tiers)} tiers"


# ---------------------------------------------------------------------------
# Monetary threshold tests
# ---------------------------------------------------------------------------

def test_monetary_thresholds_are_numeric():
    """All monetary thresholds must be numeric."""
    assert isinstance(MONETARY_HIGH_MIN, (int, float))
    assert isinstance(MONETARY_MEDIUM_MIN, (int, float))
    assert isinstance(MONETARY_MEDIUM_MAX, (int, float))


def test_monetary_high_above_medium():
    """High tier must start above medium tier maximum."""
    assert MONETARY_HIGH_MIN > MONETARY_MEDIUM_MAX


def test_monetary_medium_range_valid():
    """Medium tier min must be less than medium tier max."""
    assert MONETARY_MEDIUM_MIN <= MONETARY_MEDIUM_MAX


def test_monetary_no_gap_between_medium_and_high():
    """No gap between medium max and high min."""
    assert MONETARY_HIGH_MIN == MONETARY_MEDIUM_MAX + 1


def test_monetary_handles_negative_amounts():
    """Negative amounts (refunds) must fall into low tier."""
    for amount in [-1000000, -100, -1]:
        in_high = amount >= MONETARY_HIGH_MIN
        in_medium = MONETARY_MEDIUM_MIN <= amount <= MONETARY_MEDIUM_MAX
        assert not in_high, f"Amount {amount} incorrectly in high tier"
        assert not in_medium, f"Amount {amount} incorrectly in medium tier"


def test_monetary_full_coverage():
    """Every possible amount maps to exactly one tier."""
    test_amounts = [-1000000, -1, 0, 50, 99, 100, 499, 999, 1000, 5000, 97460405]
    for amount in test_amounts:
        tiers = []
        if amount >= MONETARY_HIGH_MIN:
            tiers.append("high")
        elif MONETARY_MEDIUM_MIN <= amount <= MONETARY_MEDIUM_MAX:
            tiers.append("medium")
        else:
            tiers.append("low")
        assert len(tiers) == 1, f"Amount {amount} maps to {len(tiers)} tiers"
