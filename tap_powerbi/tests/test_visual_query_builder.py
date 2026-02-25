"""Tests for visual_query_builder — converting Power BI visual configs to DAX."""

import base64
import json

from tap_powerbi.visual_query_builder import (
    visuals_from_report_definition,
    _extract_title,
    _prototype_to_dax,
    _infer_columns_from_proto,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_proto(from_clauses, select_clauses):
    return {"Version": 2, "From": from_clauses, "Select": select_clauses}


def _col_select(alias, prop):
    return {
        "Column": {"Expression": {"SourceRef": {"Source": alias}}, "Property": prop},
        "Name": f"E.{prop}",
    }


def _measure_select(alias, prop):
    return {
        "Measure": {"Expression": {"SourceRef": {"Source": alias}}, "Property": prop},
        "Name": f"M.{prop}",
    }


def _agg_select(alias, prop, func=0):
    return {
        "Aggregation": {
            "Expression": {
                "Column": {"Expression": {"SourceRef": {"Source": alias}}, "Property": prop}
            },
            "Function": func,
        },
        "Name": f"A.{prop}",
    }


def _make_visual_config(name, visual_type, proto, title=None):
    sv = {
        "visualType": visual_type,
        "projections": {},
        "prototypeQuery": proto,
    }
    if title:
        sv["objects"] = {"title": [{"properties": {"text": {"expr": {"Literal": {"Value": f"'{title}'"}}}}}]}
    return {"name": name, "singleVisual": sv}


def _make_report_definition(visual_configs):
    report_json = {
        "sections": [{
            "displayName": "Page 1",
            "visualContainers": [
                {"config": json.dumps(vc), "filters": "[]"}
                for vc in visual_configs
            ],
        }],
    }
    return {
        "parts": [{
            "path": "report.json",
            "payload": base64.b64encode(json.dumps(report_json).encode()).decode(),
            "payloadType": "InlineBase64",
        }],
    }


# ---------------------------------------------------------------------------
# _extract_title
# ---------------------------------------------------------------------------

def test_extract_title_from_literal():
    sv = {"objects": {"title": [{"properties": {"text": {"expr": {"Literal": {"Value": "'My Title'"}}}}}]}}
    assert _extract_title(sv) == "My Title"


def test_extract_title_missing():
    assert _extract_title({}) == ""
    assert _extract_title({"objects": {}}) == ""
    assert _extract_title({"objects": {"title": []}}) == ""


# ---------------------------------------------------------------------------
# _prototype_to_dax
# ---------------------------------------------------------------------------

def test_dax_column_and_measure():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _measure_select("t", "MeasureA")],
    )
    dax = _prototype_to_dax(proto, "barChart")
    assert dax == "EVALUATE SUMMARIZECOLUMNS('TableA'[Col1], \"MeasureA\", [MeasureA])"


def test_dax_two_columns_and_measure():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _col_select("t", "Col2"), _measure_select("t", "MeasureA")],
    )
    dax = _prototype_to_dax(proto, "columnChart")
    assert "'TableA'[Col1]" in dax
    assert "'TableA'[Col2]" in dax
    assert "\"MeasureA\", [MeasureA]" in dax


def test_dax_card():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_measure_select("t", "MeasureA")],
    )
    dax = _prototype_to_dax(proto, "card")
    assert dax == 'EVALUATE ROW("MeasureA", [MeasureA])'


def test_dax_card_multiple_measures():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_measure_select("t", "M1"), _measure_select("t", "M2")],
    )
    dax = _prototype_to_dax(proto, "card")
    assert "EVALUATE ROW(" in dax
    assert "\"M1\", [M1]" in dax
    assert "\"M2\", [M2]" in dax


def test_dax_aggregation_sum():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _agg_select("t", "Val", func=0)],
    )
    dax = _prototype_to_dax(proto, "barChart")
    assert "SUM('TableA'[Val])" in dax


def test_dax_aggregation_avg():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _agg_select("t", "Val", func=1)],
    )
    assert "AVG('TableA'[Val])" in _prototype_to_dax(proto, "barChart")


def test_dax_aggregation_count():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _agg_select("t", "Val", func=2)],
    )
    assert "COUNT('TableA'[Val])" in _prototype_to_dax(proto, "barChart")


def test_dax_columns_only():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1")],
    )
    assert _prototype_to_dax(proto, "tableEx") == "EVALUATE DISTINCT('TableA'[Col1])"


def test_dax_no_from_returns_none():
    proto = _make_proto([], [_col_select("t", "Col1")])
    assert _prototype_to_dax(proto, "barChart") is None


def test_dax_no_select_returns_none():
    proto = _make_proto([{"Name": "t", "Entity": "TableA", "Type": 0}], [])
    assert _prototype_to_dax(proto, "barChart") is None


def test_dax_measures_only_non_card():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_measure_select("t", "M1")],
    )
    dax = _prototype_to_dax(proto, "barChart")
    assert dax == 'EVALUATE ROW("M1", [M1])'


# ---------------------------------------------------------------------------
# _infer_columns_from_proto
# ---------------------------------------------------------------------------

def test_infer_column():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1")],
    )
    assert _infer_columns_from_proto(proto) == [{"name": "Col1", "dataType": "String"}]


def test_infer_measure():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_measure_select("t", "M1")],
    )
    assert _infer_columns_from_proto(proto) == [{"name": "M1", "dataType": "Double"}]


def test_infer_aggregation():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_agg_select("t", "Val", func=0)],
    )
    assert _infer_columns_from_proto(proto) == [{"name": "SUM of Val", "dataType": "Double"}]


def test_infer_mixed():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _measure_select("t", "M1")],
    )
    cols = _infer_columns_from_proto(proto)
    assert len(cols) == 2
    assert cols[0] == {"name": "Col1", "dataType": "String"}
    assert cols[1] == {"name": "M1", "dataType": "Double"}


# ---------------------------------------------------------------------------
# visuals_from_report_definition — full pipeline
# ---------------------------------------------------------------------------

def test_full_extraction():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _measure_select("t", "M1")],
    )
    vc = _make_visual_config("v1", "barChart", proto, title="My Chart")
    visuals = visuals_from_report_definition(_make_report_definition([vc]))
    assert len(visuals) == 1
    assert visuals[0]["title"] == "My Chart"
    assert visuals[0]["visual_type"] == "barChart"
    assert "SUMMARIZECOLUMNS" in visuals[0]["dax_query"]
    assert len(visuals[0]["columns"]) == 2


def test_skips_slicer():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1")],
    )
    vc = _make_visual_config("s1", "slicer", proto)
    assert visuals_from_report_definition(_make_report_definition([vc])) == []


def test_skips_textbox():
    vc = _make_visual_config("t1", "textbox", {})
    assert visuals_from_report_definition(_make_report_definition([vc])) == []


def test_skips_image():
    vc = _make_visual_config("i1", "image", {})
    assert visuals_from_report_definition(_make_report_definition([vc])) == []


def test_empty_definition():
    assert visuals_from_report_definition({}) == []
    assert visuals_from_report_definition({"parts": []}) == []


def test_multiple_visuals():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _measure_select("t", "M1")],
    )
    vc1 = _make_visual_config("v1", "barChart", proto, title="Chart A")
    vc2 = _make_visual_config("v2", "card", _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_measure_select("t", "M1")],
    ), title="Card B")
    visuals = visuals_from_report_definition(_make_report_definition([vc1, vc2]))
    assert len(visuals) == 2
    assert {v["title"] for v in visuals} == {"Chart A", "Card B"}


def test_no_title_uses_name():
    proto = _make_proto(
        [{"Name": "t", "Entity": "TableA", "Type": 0}],
        [_col_select("t", "Col1"), _measure_select("t", "M1")],
    )
    vc = _make_visual_config("myVisualName", "barChart", proto)
    visuals = visuals_from_report_definition(_make_report_definition([vc]))
    assert visuals[0]["title"] == "myVisualName"
