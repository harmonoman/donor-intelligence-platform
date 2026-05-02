"""
Schema validation tests.
Ticket 2.1 — Raw Table Schema Definition

Tests validate:
- Schema is importable and is a list
- All real FEC columns are present
- All field types are valid BigQuery types
- _load_date exists and is DATE type and REQUIRED
"""

from pipelines.ingest.schema import RAW_FEC_SCHEMA

# Valid BigQuery field types for this project
VALID_BQ_TYPES = {"STRING", "INTEGER", "FLOAT", "NUMERIC", "DATE", "TIMESTAMP", "BOOLEAN"}

# All 21 real FEC column names confirmed from data exploration (Ticket 1.1)
EXPECTED_FEC_COLUMNS = [
    "CMTE_ID",
    "AMNDT_IND",
    "RPT_TP",
    "TRANSACTION_PGI",
    "IMAGE_NUM",
    "TRANSACTION_TP",
    "ENTITY_TP",
    "NAME",
    "CITY",
    "STATE",
    "ZIP_CODE",
    "EMPLOYER",
    "OCCUPATION",
    "TRANSACTION_DT",
    "TRANSACTION_AMT",
    "OTHER_ID",
    "TRAN_ID",
    "FILE_NUM",
    "MEMO_CD",
    "MEMO_TEXT",
    "SUB_ID",
]


def test_schema_is_a_list():
    """Schema must be a list of field definitions."""
    assert isinstance(RAW_FEC_SCHEMA, list), (
        "RAW_FEC_SCHEMA must be a list"
    )


def test_schema_is_not_empty():
    """Schema must contain at least one field."""
    assert len(RAW_FEC_SCHEMA) > 0, (
        "RAW_FEC_SCHEMA is empty"
    )


def test_all_fec_columns_present():
    """All 21 real FEC columns must be in the schema."""
    schema_names = {field["name"] for field in RAW_FEC_SCHEMA}
    missing = [col for col in EXPECTED_FEC_COLUMNS if col not in schema_names]
    assert not missing, (
        f"Missing FEC columns from schema: {missing}"
    )


def test_load_date_field_exists():
    """_load_date must be present for partitioning."""
    schema_names = {field["name"] for field in RAW_FEC_SCHEMA}
    assert "_load_date" in schema_names, (
        "_load_date field is missing — required for partition overwrite"
    )


def test_load_date_is_date_type():
    """_load_date must be DATE type."""
    load_date = next(
        (f for f in RAW_FEC_SCHEMA if f["name"] == "_load_date"), None
    )
    assert load_date is not None
    assert load_date["type"] == "DATE", (
        f"_load_date must be DATE type, got: {load_date['type']}"
    )


def test_load_date_is_required():
    """_load_date must be REQUIRED mode — never nullable."""
    load_date = next(
        (f for f in RAW_FEC_SCHEMA if f["name"] == "_load_date"), None
    )
    assert load_date is not None
    assert load_date["mode"] == "REQUIRED", (
        f"_load_date must be REQUIRED mode, got: {load_date['mode']}"
    )


def test_all_fields_have_valid_types():
    """Every field must use a valid BigQuery type."""
    invalid = [
        f["name"] for f in RAW_FEC_SCHEMA
        if f.get("type") not in VALID_BQ_TYPES
    ]
    assert not invalid, (
        f"Fields with invalid BigQuery types: {invalid}"
    )


def test_all_fields_have_required_keys():
    """Every field definition must have name, type, and mode."""
    incomplete = [
        f for f in RAW_FEC_SCHEMA
        if not all(k in f for k in ("name", "type", "mode"))
    ]
    assert not incomplete, (
        f"Fields missing required keys (name/type/mode): {incomplete}"
    )


def test_no_duplicate_field_names():
    """No field name should appear more than once."""
    names = [f["name"] for f in RAW_FEC_SCHEMA]
    duplicates = [n for n in names if names.count(n) > 1]
    assert not duplicates, (
        f"Duplicate field names in schema: {set(duplicates)}"
    )


def test_transaction_amt_is_numeric():
    """TRANSACTION_AMT must be NUMERIC — never FLOAT for financial data."""
    field = next(
        (f for f in RAW_FEC_SCHEMA if f["name"] == "TRANSACTION_AMT"), None
    )
    assert field is not None
    assert field["type"] == "NUMERIC", (
        f"TRANSACTION_AMT must be NUMERIC for financial precision, got: {field['type']}"
    )
