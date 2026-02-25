"""Map Power BI column data types to JSON Schema types."""

POWERBI_TYPE_MAP = {
    "String": {"type": ["string", "null"]},
    "Int64": {"type": ["number", "null"]},
    "Double": {"type": ["number", "null"]},
    "Decimal": {"type": ["number", "null"]},
    "Boolean": {"type": ["boolean", "null"]},
    "DateTime": {"type": ["string", "null"], "format": "date-time"},
}


def powerbi_type_to_jsonschema(powerbi_type: str) -> dict:
    """Convert a Power BI column data type to a JSON Schema type definition."""
    return POWERBI_TYPE_MAP.get(powerbi_type, {"type": ["string", "null"]})


def build_schema_from_columns(columns: list) -> dict:
    """Build a JSON Schema object from a list of Power BI column definitions.

    Each column dict has keys: 'name' and 'dataType'.
    """
    properties = {}
    for col in columns:
        properties[col["name"]] = powerbi_type_to_jsonschema(col["dataType"])
    return {"type": "object", "properties": properties}
