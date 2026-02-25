"""Convert Power BI report visual configurations into executable DAX queries.

Parses the prototype query embedded in each visual container and produces
a SUMMARIZECOLUMNS / ROW DAX statement that returns the same data the
visual renders in Power BI.
"""

import base64
import json
import logging

logger = logging.getLogger(__name__)


def visuals_from_report_definition(definition: dict) -> list[dict]:
    """Extract visual metadata from a Fabric report getDefinition response.

    Returns a list of dicts:
        [{"name": str, "title": str, "visual_type": str,
          "dax_query": str, "columns": list[dict]}]
    """
    report_json = None
    for part in definition.get("parts", []):
        if part.get("path") == "report.json":
            report_json = json.loads(
                base64.b64decode(part["payload"]).decode("utf-8")
            )
            break

    if not report_json:
        return []

    visuals = []
    for section in report_json.get("sections", []):
        page_name = section.get("displayName", "Page")
        for vc in section.get("visualContainers", []):
            config = json.loads(vc.get("config", "{}"))
            sv = config.get("singleVisual", {})
            if not sv:
                continue

            visual_type = sv.get("visualType", "unknown")
            visual_name = config.get("name", "unnamed")
            title = _extract_title(sv) or visual_name
            proto = sv.get("prototypeQuery", {})

            # Skip non-data visuals
            if visual_type in ("slicer", "textbox", "shape", "image", "actionButton"):
                continue

            dax = _prototype_to_dax(proto, visual_type)
            if not dax:
                logger.debug(f"Could not build DAX for visual '{title}' ({visual_type})")
                continue

            columns = _infer_columns_from_proto(proto)

            visuals.append({
                "name": visual_name,
                "title": title,
                "visual_type": visual_type,
                "page": page_name,
                "dax_query": dax,
                "columns": columns,
            })

    return visuals


def _extract_title(single_visual: dict) -> str:
    """Pull the title text from a visual's objects, if set."""
    try:
        title_props = single_visual["objects"]["title"][0]["properties"]
        return title_props["text"]["expr"]["Literal"]["Value"].strip("'")
    except (KeyError, IndexError, TypeError):
        return ""


def _prototype_to_dax(proto: dict, visual_type: str) -> str | None:
    """Convert a prototypeQuery into a DAX string."""
    from_clauses = proto.get("From", [])
    select_clauses = proto.get("Select", [])
    if not from_clauses or not select_clauses:
        return None

    # Build source alias map: alias -> entity name
    alias_map = {f["Name"]: f["Entity"] for f in from_clauses}

    columns = []
    measures = []

    for sel in select_clauses:
        if "Column" in sel:
            source = sel["Column"]["Expression"]["SourceRef"]["Source"]
            entity = alias_map.get(source, source)
            prop = sel["Column"]["Property"]
            columns.append(f"'{entity}'[{prop}]")
        elif "Measure" in sel:
            source = sel["Measure"]["Expression"]["SourceRef"]["Source"]
            prop = sel["Measure"]["Property"]
            measures.append((f'"{prop}"', f"[{prop}]"))
        elif "Aggregation" in sel:
            source = sel["Aggregation"]["Expression"]["Column"]["Expression"]["SourceRef"]["Source"]
            entity = alias_map.get(source, source)
            prop = sel["Aggregation"]["Expression"]["Column"]["Property"]
            func_id = sel["Aggregation"].get("Function", 0)
            agg_funcs = {0: "SUM", 1: "AVG", 2: "COUNT", 3: "MIN", 4: "MAX"}
            agg = agg_funcs.get(func_id, "SUM")
            measures.append((f'"{agg} of {prop}"', f"{agg}('{entity}'[{prop}])"))

    # Card visuals: just evaluate measures
    if visual_type == "card" or (not columns and measures):
        measure_parts = ", ".join(f"{name}, {expr}" for name, expr in measures)
        return f"EVALUATE ROW({measure_parts})" if measure_parts else None

    col_refs = ", ".join(columns)
    measure_parts = ", ".join(f"{name}, {expr}" for name, expr in measures)

    if col_refs and measure_parts:
        return f"EVALUATE SUMMARIZECOLUMNS({col_refs}, {measure_parts})"
    elif col_refs:
        return f"EVALUATE DISTINCT({col_refs})"
    return None


def _infer_columns_from_proto(proto: dict) -> list[dict]:
    """Infer output column names and types from a prototype query."""
    columns = []
    alias_map = {f["Name"]: f["Entity"] for f in proto.get("From", [])}

    for sel in proto.get("Select", []):
        if "Column" in sel:
            prop = sel["Column"]["Property"]
            columns.append({"name": prop, "dataType": "String"})
        elif "Measure" in sel:
            prop = sel["Measure"]["Property"]
            columns.append({"name": prop, "dataType": "Double"})
        elif "Aggregation" in sel:
            prop = sel["Aggregation"]["Expression"]["Column"]["Property"]
            func_id = sel["Aggregation"].get("Function", 0)
            agg_funcs = {0: "SUM", 1: "AVG", 2: "COUNT", 3: "MIN", 4: "MAX"}
            agg = agg_funcs.get(func_id, "SUM")
            columns.append({"name": f"{agg} of {prop}", "dataType": "Double"})

    return columns
