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


def test_strips_table_bracket_format():
    raw = {"MyTable[Col1]": "foo", "MyTable[Col2]": 42}
    assert flatten_row(raw) == {"Col1": "foo", "Col2": 42}


def test_strips_bare_bracket_format():
    raw = {"[Measure A]": 100.5, "[Measure B]": 200}
    assert flatten_row(raw) == {"Measure A": 100.5, "Measure B": 200}


def test_mixed_visual_output():
    raw = {"MyTable[Col1]": "foo", "[Measure A]": 99.9, "plain": True}
    assert flatten_row(raw) == {"Col1": "foo", "Measure A": 99.9, "plain": True}
