"""Tests for Power BI to JSON Schema type mapping."""

from tap_powerbi.type_mapping import powerbi_type_to_jsonschema, build_schema_from_columns


def test_string_type():
    assert powerbi_type_to_jsonschema("String") == {"type": ["string", "null"]}


def test_int64_type():
    assert powerbi_type_to_jsonschema("Int64") == {"type": ["number", "null"]}


def test_double_type():
    assert powerbi_type_to_jsonschema("Double") == {"type": ["number", "null"]}


def test_decimal_type():
    assert powerbi_type_to_jsonschema("Decimal") == {"type": ["number", "null"]}


def test_boolean_type():
    assert powerbi_type_to_jsonschema("Boolean") == {"type": ["boolean", "null"]}


def test_datetime_type():
    assert powerbi_type_to_jsonschema("DateTime") == {
        "type": ["string", "null"],
        "format": "date-time",
    }


def test_unknown_type_defaults_to_string():
    assert powerbi_type_to_jsonschema("SomeNewType") == {"type": ["string", "null"]}


def test_build_schema_from_columns():
    columns = [
        {"name": "Region", "dataType": "String"},
        {"name": "Revenue", "dataType": "Double"},
        {"name": "IsActive", "dataType": "Boolean"},
    ]
    schema = build_schema_from_columns(columns)
    assert schema == {
        "type": "object",
        "properties": {
            "Region": {"type": ["string", "null"]},
            "Revenue": {"type": ["number", "null"]},
            "IsActive": {"type": ["boolean", "null"]},
        },
    }


def test_build_schema_empty_columns():
    schema = build_schema_from_columns([])
    assert schema == {"type": "object", "properties": {}}
