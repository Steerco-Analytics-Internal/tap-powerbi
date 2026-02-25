"""Tests for row flattening (stripping Power BI column prefixes)."""

from tap_powerbi.row_flattener import flatten_row


def test_strips_table_prefix():
    raw = {"[Items].[Label]": "foo", "[Items].[Value]": 42}
    assert flatten_row(raw) == {"Label": "foo", "Value": 42}


def test_handles_no_prefix():
    raw = {"Label": "foo", "Value": 42}
    assert flatten_row(raw) == {"Label": "foo", "Value": 42}


def test_handles_mixed():
    raw = {"[Items].[Label]": "foo", "plain_col": 123}
    assert flatten_row(raw) == {"Label": "foo", "plain_col": 123}


def test_handles_empty_row():
    assert flatten_row({}) == {}


def test_preserves_none_values():
    raw = {"[T].[Col]": None}
    assert flatten_row(raw) == {"Col": None}
