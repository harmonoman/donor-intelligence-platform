"""
Shared Normalization Utility
Ticket 3.1 — Shared Normalization Utility

This is the SINGLE SOURCE OF TRUTH for all normalization logic.

Used by:
    - staging layer (stg_contributions)
    - identity resolution layer (dim_donors matching)

Critical contract:
    The same input MUST always produce the same output.
    If normalization is inconsistent between layers,
    identity resolution will silently fail to match
    records that represent the same real donor.

    Example of silent failure:
        Staging produces:  "smith john"
        Identity expects:  "smith, john"  ← comma not removed
        Result: no match → duplicate donor ID created

Never duplicate this logic elsewhere.
Always import from this module.

Known limitation:
    "00000" is a valid FEC placeholder for missing ZIP code.
    This function returns "00000" unchanged — callers should
    treat this as a null ZIP in identity resolution logic.

Known limitation:
    normalize_address does not remove hyphens (unlike normalize_name).
    This is intentional — hyphens in addresses are meaningful
    (e.g., "Suite 1-A"). Document any address hyphen handling
    in the staging SQL explicitly.
"""

import re

# ---------------------------------------------------------------------------
# Abbreviation map for address normalization
# Order matters — longer strings before shorter to prevent partial matches
# ---------------------------------------------------------------------------

ADDRESS_ABBREVIATIONS = {
    "street": "st",
    "avenue": "ave",
    "boulevard": "blvd",
    "road": "rd",
    "drive": "dr",
    "court": "ct",
    "lane": "ln",
    "place": "pl",
    "circle": "cir",
}


def normalize_name(name: str) -> str:
    """
    Normalize a donor name for consistent matching.

    Rules applied in order:
        1. Handle None → return empty string
        2. Lowercase
        3. Remove punctuation (periods, commas, apostrophes)
        4. Replace hyphens with spaces
        5. Strip leading/trailing whitespace
        6. Collapse internal whitespace to single space

    FEC names arrive as "LAST, FIRST" — the comma is treated as
    punctuation and removed, producing "last first".

    Examples:
        "SMITH, JOHN"       → "smith john"
        "O'NEIL, PATRICK"   → "oneil patrick"
        "MARY-SUE JONES"    → "mary sue jones"
        "John A. Doe Jr."   → "john a doe jr"
    """
    if not name:
        return ""

    name = name.lower()
    name = name.replace("-", " ")           # hyphens → spaces
    name = name.replace("_", " ")           # underscores → spaces
    name = re.sub(r"[^a-z0-9\s]", "", name)  # after lowercasing — explicitly allow only alphanumeric and space
    name = re.sub(r"\s+", " ", name)        # collapse whitespace
    name = name.strip()

    return name


def normalize_address(address: str) -> str:
    """
    Normalize a street address for consistent matching.

    Rules applied in order:
        1. Handle None → return empty string
        2. Lowercase
        3. Remove punctuation (periods, commas)
        4. Strip whitespace
        5. Standardize street type abbreviations

    Examples:
        "123 Main Street"       → "123 main st"
        "456 Elm Avenue"        → "456 elm ave"
        "789 Oak Rd."           → "789 oak rd"
        "100 Sunset Boulevard"  → "100 sunset blvd"
    """
    if not address:
        return ""

    address = address.lower()
    address = re.sub(r"[^\w\s]", "", address)   # remove punctuation
    address = re.sub(r"\s+", " ", address)       # collapse whitespace
    address = address.strip()

    # Apply abbreviations — match whole words only
    for full, abbrev in ADDRESS_ABBREVIATIONS.items():
        address = re.sub(rf"\b{full}\b", abbrev, address)

    return address


def normalize_zip(zip_code: str) -> str:
    """
    Normalize a ZIP code to 5-digit format.

    Rules applied in order:
        1. Handle None → return empty string
        2. Strip whitespace
        3. Remove all non-numeric characters (handles ZIP+4 with/without hyphen)
        4. Take first 5 digits (truncates ZIP+4)
        5. Left-pad with zeros if fewer than 5 digits

    Decision: short ZIPs (< 5 digits) are left-padded with zeros.
    This handles leading-zero ZIPs from Northeast states (e.g., 02101 → "02101")
    that spreadsheet tools sometimes strip.

    Examples:
        "37209-1234"  → "37209"
        "372091234"   → "37209"
        " 37209 "     → "37209"
        "3720"        → "03720"
        "02101"       → "02101"
    """
    if not zip_code:
        return ""

    # Remove all non-numeric characters (strips hyphens, spaces, letters)
    digits = re.sub(r"\D", "", str(zip_code).strip())

    if not digits:
        return ""

    # Truncate to 5 digits, then left-pad if shorter
    return digits[:5].zfill(5)
