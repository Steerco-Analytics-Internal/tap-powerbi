"""Flatten Power BI row data by stripping [Table].[Column] prefixes."""

import re

_PREFIX_PATTERN = re.compile(r"^\[.*?\]\.\[(.+)\]$")


def flatten_row(row: dict) -> dict:
    """Strip [TableName].[ColumnName] prefixes from a Power BI row dict."""
    cleaned = {}
    for key, value in row.items():
        match = _PREFIX_PATTERN.match(key)
        cleaned[match.group(1) if match else key] = value
    return cleaned
