"""Flatten Power BI row data by stripping table/column prefixes.

Power BI executeQueries returns keys in several formats depending on the DAX:
  - Table columns:  "TableName[ColumnName]"  or  "[Table].[Column]"
  - Measures:       "[Measure Name]"
  - Aliased:        "[alias]"

This module normalises them all to simple column names.
"""

import re

# Matches [Table].[Column]
_BRACKET_DOT_PATTERN = re.compile(r"^\[.*?\]\.\[(.+)\]$")
# Matches TableName[ColumnName]
_TABLE_BRACKET_PATTERN = re.compile(r"^.+?\[(.+)\]$")
# Matches [ColumnName]
_BARE_BRACKET_PATTERN = re.compile(r"^\[(.+)\]$")


def flatten_row(row: dict) -> dict:
    """Strip table/column prefixes from a Power BI row dict."""
    cleaned = {}
    for key, value in row.items():
        match = (
            _BRACKET_DOT_PATTERN.match(key)
            or _TABLE_BRACKET_PATTERN.match(key)
            or _BARE_BRACKET_PATTERN.match(key)
        )
        cleaned[match.group(1) if match else key] = value
    return cleaned
