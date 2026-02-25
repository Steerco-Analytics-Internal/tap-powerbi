"""Parse TMDL (Tabular Model Definition Language) table definitions.

Used to extract table names, column names, and data types from the
base64-encoded TMDL files returned by the Fabric getDefinition API.
"""

import base64
import re

# TMDL uses lowercase type names; map to Power BI REST API type names.
_TMDL_TYPE_MAP = {
    "string": "String",
    "int64": "Int64",
    "double": "Double",
    "decimal": "Decimal",
    "boolean": "Boolean",
    "datetime": "DateTime",
    "binary": "String",
}


def _tmdl_type_to_powerbi_type(tmdl_type: str) -> str:
    """Convert a TMDL dataType value to a Power BI type name."""
    return _TMDL_TYPE_MAP.get(tmdl_type.lower(), "String")


def _unquote_tmdl_name(name: str) -> str:
    """Remove surrounding single-quotes from a TMDL object name."""
    name = name.strip()
    if name.startswith("'") and name.endswith("'"):
        return name[1:-1].replace("''", "'")
    return name


def parse_tmdl_table(tmdl_text: str) -> dict | None:
    """Parse a single TMDL table file and return table metadata.

    Returns a dict with keys: name, columns (list of {name, dataType}),
    isHidden (bool).  Returns None if the text has no table declaration.
    """
    lines = tmdl_text.split("\n")
    table_name = None
    table_hidden = False
    columns: list[dict] = []

    # State tracking for the current child object.
    in_column = False
    col_name: str | None = None
    col_type = "String"
    col_hidden = False

    def _flush_column():
        nonlocal col_name, col_type, col_hidden, in_column
        if in_column and col_name and not col_hidden:
            columns.append({"name": col_name, "dataType": col_type})
        in_column = False
        col_name = None
        col_type = "String"
        col_hidden = False

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("///"):
            continue

        # Table declaration (always at root level, no indent).
        if stripped.startswith("table "):
            table_name = _unquote_tmdl_name(stripped[6:])
            continue

        # Column declaration.
        if stripped.startswith("column "):
            _flush_column()
            in_column = True
            col_decl = stripped[7:].strip()
            # Calculated columns: `column 'Name' = EXPRESSION`
            col_name = _unquote_tmdl_name(col_decl.split("=")[0])
            col_type = "String"
            col_hidden = False
            continue

        # Other child objects end column context.
        if stripped.startswith(
            ("measure ", "partition ", "hierarchy ", "annotation ", "calculationGroup")
        ):
            _flush_column()
            continue

        # Properties.
        if in_column:
            if stripped.startswith("dataType:"):
                col_type = _tmdl_type_to_powerbi_type(stripped.split(":", 1)[1].strip())
            elif stripped == "isHidden":
                col_hidden = True
        elif table_name is not None and stripped == "isHidden":
            table_hidden = True

    _flush_column()

    if table_name is None:
        return None
    return {"name": table_name, "columns": columns, "isHidden": table_hidden}


def tables_from_definition(definition: dict) -> list[dict]:
    """Extract visible tables from a Fabric getDefinition response.

    ``definition`` is the ``"definition"`` object from the API response,
    containing a ``"parts"`` list of {path, payload, payloadType} dicts.

    Returns a list of dicts compatible with the REST ``/tables`` format:
    ``[{"name": "...", "columns": [{"name": "...", "dataType": "..."}]}]``
    """
    tables: list[dict] = []
    for part in definition.get("parts", []):
        path = part.get("path", "")
        if not (path.startswith("definition/tables/") and path.endswith(".tmdl")):
            continue
        payload = base64.b64decode(part["payload"]).decode("utf-8")
        table = parse_tmdl_table(payload)
        if table and not table.get("isHidden", False):
            tables.append({
                "name": table["name"],
                "columns": table.get("columns", []),
            })
    return tables
