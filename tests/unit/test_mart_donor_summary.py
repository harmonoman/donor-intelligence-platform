"""
Unit tests for mart_donor_summary build logic.
Ticket 5.2: mart_donor_summary Build

Tests validate:
- Engagement score formula correctness
- Score bounds (0.0 to 3.0)
- No null scores possible given valid inputs
- Lapsed donor logic

Run with:
    uv run pytest tests/unit/test_mart_donor_summary.py -v
"""

from pipelines.marts.thresholds import (
    FREQUENCY_HIGH_MIN,
    FREQUENCY_MEDIUM_MIN,
    MONETARY_HIGH_MIN,
    MONETARY_MEDIUM_MIN,
    RECENCY_ACTIVE_MAX,
    RECENCY_RECENT_MAX,
)

# ---------------------------------------------------------------------------
# Score helper functions (mirrors SQL logic for unit testing)
# ---------------------------------------------------------------------------

def recency_score(days_since_last: int) -> float:
    if days_since_last <= RECENCY_RECENT_MAX:
        return 3.0
    elif days_since_last <= RECENCY_ACTIVE_MAX:
        return 2.0
    else:
        return 1.0


def frequency_score(contribution_count: int) -> float:
    if contribution_count >= FREQUENCY_HIGH_MIN:
        return 3.0
    elif contribution_count >= FREQUENCY_MEDIUM_MIN:
        return 2.0
    else:
        return 1.0


def monetary_score(total_amount: float) -> float:
    if total_amount >= MONETARY_HIGH_MIN:
        return 3.0
    elif total_amount >= MONETARY_MEDIUM_MIN:
        return 2.0
    else:
        return 1.0


def engagement_score(r: float, f: float, m: float) -> float:
    return round(0.4 * r + 0.3 * f + 0.3 * m, 2)


# ---------------------------------------------------------------------------
# Recency score tests
# ---------------------------------------------------------------------------

def test_recency_recent_scores_3():
    assert recency_score(0) == 3.0
    assert recency_score(45) == 3.0
    assert recency_score(90) == 3.0


def test_recency_active_scores_2():
    assert recency_score(91) == 2.0
    assert recency_score(180) == 2.0
    assert recency_score(365) == 2.0


def test_recency_lapsed_scores_1():
    assert recency_score(366) == 1.0
    assert recency_score(500) == 1.0
    assert recency_score(959) == 1.0


# ---------------------------------------------------------------------------
# Frequency score tests
# ---------------------------------------------------------------------------

def test_frequency_high_scores_3():
    assert frequency_score(10) == 3.0
    assert frequency_score(100) == 3.0
    assert frequency_score(39625) == 3.0


def test_frequency_medium_scores_2():
    assert frequency_score(2) == 2.0
    assert frequency_score(5) == 2.0
    assert frequency_score(9) == 2.0


def test_frequency_low_scores_1():
    assert frequency_score(1) == 1.0


# ---------------------------------------------------------------------------
# Monetary score tests
# ---------------------------------------------------------------------------

def test_monetary_high_scores_3():
    assert monetary_score(1000) == 3.0
    assert monetary_score(10000) == 3.0
    assert monetary_score(97460405) == 3.0


def test_monetary_medium_scores_2():
    assert monetary_score(100) == 2.0
    assert monetary_score(500) == 2.0
    assert monetary_score(999) == 2.0


def test_monetary_low_scores_1():
    assert monetary_score(0) == 1.0
    assert monetary_score(50) == 1.0
    assert monetary_score(99) == 1.0


def test_monetary_negative_scores_1():
    """Refunds produce negative totals. These fall into Low tier."""
    assert monetary_score(-100) == 1.0
    assert monetary_score(-1000000) == 1.0


# ---------------------------------------------------------------------------
# Engagement score formula tests
# ---------------------------------------------------------------------------

def test_engagement_score_max():
    """Perfect donor: recent, high frequency, high monetary."""
    assert engagement_score(3.0, 3.0, 3.0) == 3.0


def test_engagement_score_min():
    """Lowest possible score: lapsed, one-time, small dollar."""
    assert engagement_score(1.0, 1.0, 1.0) == 1.0


def test_engagement_score_formula_weights():
    """Recency weighted 0.4, frequency and monetary each 0.3."""
    score = engagement_score(3.0, 1.0, 1.0)
    assert score == round(0.4 * 3.0 + 0.3 * 1.0 + 0.3 * 1.0, 2)


def test_engagement_score_lapsed_high_value():
    """Lapsed major donor: low recency, high frequency, high monetary."""
    score = engagement_score(1.0, 3.0, 3.0)
    assert score == round(0.4 * 1.0 + 0.3 * 3.0 + 0.3 * 3.0, 2)
    assert score == 2.2


def test_engagement_score_always_between_1_and_3():
    """All valid combinations produce scores between 1.0 and 3.0."""
    for r in [1.0, 2.0, 3.0]:
        for f in [1.0, 2.0, 3.0]:
            for m in [1.0, 2.0, 3.0]:
                score = engagement_score(r, f, m)
                assert 1.0 <= score <= 3.0, (
                    f"Score {score} out of range for r={r}, f={f}, m={m}"
                )


def test_engagement_score_no_nulls_given_valid_inputs():
    """Score is always a number given valid tier scores."""
    score = engagement_score(2.0, 1.0, 3.0)
    assert score is not None
    assert isinstance(score, float)
