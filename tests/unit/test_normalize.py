"""
Normalization utility tests.
Ticket 3.1 — Shared Normalization Utility

These tests define the normalization CONTRACT.
If any test changes, the contract has changed.
Both staging AND identity resolution depend on this behavior being stable.

Run with:
    uv run pytest tests/unit/test_normalize.py -v
"""

from pipelines.utils.normalize import normalize_address, normalize_name, normalize_zip

# ---------------------------------------------------------------------------
# normalize_name tests
# ---------------------------------------------------------------------------

def test_name_lowercased():
    """All uppercase names are lowercased."""
    assert normalize_name("JOHN DOE") == "john doe"


def test_name_strips_whitespace():
    """Leading and trailing whitespace is removed."""
    assert normalize_name("  MARY SUE  ") == "mary sue"


def test_name_removes_punctuation():
    """Punctuation is removed — periods, apostrophes, commas."""
    assert normalize_name("John A. Doe Jr.") == "john a doe jr"


def test_name_removes_apostrophe():
    """Apostrophes in names like O'NEIL are removed."""
    assert normalize_name("O'NEIL, PATRICK") == "oneil patrick"


def test_name_handles_hyphenated_name():
    """Hyphens in names are replaced with a space."""
    assert normalize_name("MARY-SUE JONES") == "mary sue jones"


def test_name_normalizes_fec_last_first_format():
    """FEC names arrive as LAST, FIRST — comma removed, lowercased."""
    assert normalize_name("SMITH, JOHN") == "smith john"


def test_name_collapses_extra_spaces():
    """Multiple internal spaces are collapsed to single space."""
    assert normalize_name("JOHN  A   DOE") == "john a doe"


def test_name_empty_string():
    """Empty string returns empty string."""
    assert normalize_name("") == ""


def test_name_none_returns_empty():
    """None input returns empty string — safe for null FEC fields."""
    assert normalize_name(None) == ""


def test_name_with_underscore():
    """Underscores are removed — not treated as word characters."""
    assert normalize_name("JOHN_DOE") == "john doe"


def test_name_all_whitespace():
    """All-whitespace input returns empty string."""
    assert normalize_name("   ") == ""


def test_name_only_punctuation():
    """All-punctuation input returns empty string."""
    assert normalize_name("---") == ""


def test_name_numeric_suffix():
    """Numeric suffixes like III are preserved."""
    assert normalize_name("DOE, JOHN III") == "doe john iii"


# ---------------------------------------------------------------------------
# normalize_address tests
# ---------------------------------------------------------------------------

def test_address_lowercased():
    """Address is lowercased."""
    assert normalize_address("123 MAIN STREET") == "123 main st"


def test_address_street_abbreviated():
    """'street' is replaced with 'st'."""
    assert normalize_address("123 Main Street") == "123 main st"


def test_address_avenue_abbreviated():
    """'avenue' is replaced with 'ave'."""
    assert normalize_address("456 Elm Avenue") == "456 elm ave"


def test_address_road_abbreviated():
    """'road' is replaced with 'rd'."""
    assert normalize_address("789 Oak Road") == "789 oak rd"


def test_address_boulevard_abbreviated():
    """'boulevard' is replaced with 'blvd'."""
    assert normalize_address("100 Sunset Boulevard") == "100 sunset blvd"


def test_address_strips_whitespace():
    """Leading and trailing whitespace is removed."""
    assert normalize_address("  123 Main St  ") == "123 main st"


def test_address_removes_periods():
    """Trailing periods on abbreviations are removed."""
    assert normalize_address("456 Elm Rd.") == "456 elm rd"


def test_address_already_abbreviated():
    """Already abbreviated addresses are not double-abbreviated."""
    assert normalize_address("123 Main St") == "123 main st"


def test_address_empty_string():
    """Empty string returns empty string."""
    assert normalize_address("") == ""


def test_address_none_returns_empty():
    """None input returns empty string."""
    assert normalize_address(None) == ""


def test_address_with_apartment():
    """Apartment units are preserved in normalized address."""
    assert normalize_address("123 Main St Apt 4B") == "123 main st apt 4b"


def test_address_with_directional():
    """Directional prefixes are preserved."""
    assert normalize_address("123 N Main Street") == "123 n main st"


# ---------------------------------------------------------------------------
# normalize_zip tests
# ---------------------------------------------------------------------------

def test_zip_strips_whitespace():
    """Leading and trailing whitespace removed."""
    assert normalize_zip(" 37209 ") == "37209"


def test_zip_strips_zip4_with_hyphen():
    """ZIP+4 with hyphen is truncated to 5 digits."""
    assert normalize_zip("37209-1234") == "37209"


def test_zip_strips_zip4_without_hyphen():
    """9-digit ZIP without hyphen is truncated to 5 digits."""
    assert normalize_zip("372091234") == "37209"


def test_zip_pads_short_zip():
    """Short ZIPs are left-padded with zeros to 5 digits."""
    assert normalize_zip("3720") == "03720"


def test_zip_already_valid():
    """Valid 5-digit ZIP passes through unchanged."""
    assert normalize_zip("30301") == "30301"


def test_zip_empty_string():
    """Empty string returns empty string."""
    assert normalize_zip("") == ""


def test_zip_none_returns_empty():
    """None returns empty string."""
    assert normalize_zip(None) == ""


def test_zip_removes_non_numeric():
    """Non-numeric characters are stripped before processing."""
    assert normalize_zip("3720 1") == "37201"


def test_zip_all_non_numeric():
    """Fully non-numeric input returns empty string."""
    assert normalize_zip("INVALID") == ""


def test_zip_all_punctuation():
    """All-punctuation input returns empty string."""
    assert normalize_zip("---") == ""


def test_zip_placeholder_zeros():
    """'00000' is a known FEC placeholder for missing ZIP — returned as-is."""
    assert normalize_zip("00000") == "00000"
