"""Tests for TMDL parsing."""

import base64

from tap_powerbi.tmdl_parser import parse_tmdl_table, tables_from_definition


# --- parse_tmdl_table ---

def test_parses_table_with_columns():
    tmdl = """\
table Items

    column Label
        dataType: string
        sourceColumn: Label

    column Value
        dataType: double
        sourceColumn: Value
"""
    result = parse_tmdl_table(tmdl)
    assert result["name"] == "Items"
    assert result["isHidden"] is False
    assert len(result["columns"]) == 2
    assert result["columns"][0] == {"name": "Label", "dataType": "String"}
    assert result["columns"][1] == {"name": "Value", "dataType": "Double"}


def test_parses_quoted_table_name():
    tmdl = """\
table 'My Table'

    column Id
        dataType: int64
"""
    result = parse_tmdl_table(tmdl)
    assert result["name"] == "My Table"


def test_parses_quoted_column_name():
    tmdl = """\
table Items

    column 'Product Key'
        dataType: int64
        sourceColumn: ProductKey
"""
    result = parse_tmdl_table(tmdl)
    assert result["columns"][0]["name"] == "Product Key"
    assert result["columns"][0]["dataType"] == "Int64"


def test_excludes_hidden_columns():
    tmdl = """\
table Items

    column Label
        dataType: string

    column InternalId
        dataType: int64
        isHidden

    column Value
        dataType: double
"""
    result = parse_tmdl_table(tmdl)
    names = [c["name"] for c in result["columns"]]
    assert "Label" in names
    assert "Value" in names
    assert "InternalId" not in names


def test_detects_hidden_table():
    tmdl = """\
table InternalCalc
    isHidden

    column Temp
        dataType: string
"""
    result = parse_tmdl_table(tmdl)
    assert result["isHidden"] is True


def test_handles_calculated_columns():
    tmdl = """\
table Items

    column 'Full Name' = [First] & " " & [Last]
        dataType: string

    column Value
        dataType: double
"""
    result = parse_tmdl_table(tmdl)
    assert result["columns"][0]["name"] == "Full Name"
    assert result["columns"][0]["dataType"] == "String"


def test_skips_measures():
    tmdl = """\
table Items

    column Label
        dataType: string

    measure 'Total Value' = SUM(Items[Value])
        formatString: #,##0

    column Value
        dataType: double
"""
    result = parse_tmdl_table(tmdl)
    names = [c["name"] for c in result["columns"]]
    assert names == ["Label", "Value"]


def test_handles_all_data_types():
    tmdl = """\
table TypeTest

    column A
        dataType: string
    column B
        dataType: int64
    column C
        dataType: double
    column D
        dataType: decimal
    column E
        dataType: boolean
    column F
        dataType: dateTime
"""
    result = parse_tmdl_table(tmdl)
    types = {c["name"]: c["dataType"] for c in result["columns"]}
    assert types == {
        "A": "String",
        "B": "Int64",
        "C": "Double",
        "D": "Decimal",
        "E": "Boolean",
        "F": "DateTime",
    }


def test_defaults_unknown_type_to_string():
    tmdl = """\
table Items

    column Blob
        dataType: binary
"""
    result = parse_tmdl_table(tmdl)
    assert result["columns"][0]["dataType"] == "String"


def test_returns_none_for_empty_text():
    assert parse_tmdl_table("") is None
    assert parse_tmdl_table("/// just a comment") is None


def test_handles_descriptions():
    tmdl = """\
/// Table description
table Items

    /// Column description
    column Label
        dataType: string
"""
    result = parse_tmdl_table(tmdl)
    assert result["name"] == "Items"
    assert result["columns"][0]["name"] == "Label"


# --- tables_from_definition ---

def _encode(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def test_tables_from_definition_extracts_tables():
    definition = {
        "parts": [
            {
                "path": "definition/database.tmdl",
                "payload": _encode("database TestDB"),
                "payloadType": "InlineBase64",
            },
            {
                "path": "definition/model.tmdl",
                "payload": _encode("model Model"),
                "payloadType": "InlineBase64",
            },
            {
                "path": "definition/tables/Items.tmdl",
                "payload": _encode(
                    "table Items\n\n    column Label\n        dataType: string\n\n    column Value\n        dataType: double\n"
                ),
                "payloadType": "InlineBase64",
            },
            {
                "path": "definition.pbism",
                "payload": _encode("{}"),
                "payloadType": "InlineBase64",
            },
        ]
    }
    tables = tables_from_definition(definition)
    assert len(tables) == 1
    assert tables[0]["name"] == "Items"
    assert len(tables[0]["columns"]) == 2


def test_tables_from_definition_excludes_hidden():
    definition = {
        "parts": [
            {
                "path": "definition/tables/Visible.tmdl",
                "payload": _encode("table Visible\n\n    column A\n        dataType: string\n"),
                "payloadType": "InlineBase64",
            },
            {
                "path": "definition/tables/Hidden.tmdl",
                "payload": _encode("table Hidden\n    isHidden\n\n    column B\n        dataType: string\n"),
                "payloadType": "InlineBase64",
            },
        ]
    }
    tables = tables_from_definition(definition)
    assert len(tables) == 1
    assert tables[0]["name"] == "Visible"


def test_tables_from_definition_ignores_non_table_parts():
    definition = {
        "parts": [
            {
                "path": "definition/model.tmdl",
                "payload": _encode("model Model"),
                "payloadType": "InlineBase64",
            },
            {
                "path": ".platform",
                "payload": _encode("{}"),
                "payloadType": "InlineBase64",
            },
        ]
    }
    tables = tables_from_definition(definition)
    assert tables == []
