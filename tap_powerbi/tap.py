"""PowerBI tap class."""

import logging
from typing import List

import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_powerbi.auth import get_access_token
from tap_powerbi.streams import (
    WorkspacesStream,
    DatasetsStream,
    DatasetTablesStream,
    TableDataStream,
)

logger = logging.getLogger(__name__)

API_BASE = "https://api.powerbi.com/v1.0/myorg"


def _discover_tables_via_rest(ws_id, ds_id, headers):
    """Try GET /tables endpoint (works for push datasets)."""
    resp = requests.get(
        f"{API_BASE}/groups/{ws_id}/datasets/{ds_id}/tables",
        headers=headers,
    )
    resp.raise_for_status()
    return resp.json().get("value", [])


def _discover_tables_via_dax(ws_id, ds_id, headers):
    """Fallback: use DAX INFO.TABLES() + INFO.COLUMNS() for imported/DirectQuery datasets."""
    # Discover table names
    tables_resp = requests.post(
        f"{API_BASE}/groups/{ws_id}/datasets/{ds_id}/executeQueries",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "queries": [{"query": "EVALUATE INFO.TABLES()"}],
            "serializerSettings": {"includeNulls": True},
        },
    )
    tables_resp.raise_for_status()

    tables_data = tables_resp.json()
    raw_tables = []
    for result in tables_data.get("results", []):
        for table in result.get("tables", []):
            raw_tables.extend(table.get("rows", []))

    # Filter to user-facing tables (not internal/hidden)
    table_names = []
    for t in raw_tables:
        name = t.get("[Name]", t.get("Name"))
        is_hidden = t.get("[IsHidden]", t.get("IsHidden", False))
        if name and not is_hidden:
            table_names.append(name)

    if not table_names:
        return []

    # Discover columns for all tables
    cols_resp = requests.post(
        f"{API_BASE}/groups/{ws_id}/datasets/{ds_id}/executeQueries",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "queries": [{"query": "EVALUATE INFO.COLUMNS()"}],
            "serializerSettings": {"includeNulls": True},
        },
    )
    cols_resp.raise_for_status()

    cols_data = cols_resp.json()
    raw_cols = []
    for result in cols_data.get("results", []):
        for table in result.get("tables", []):
            raw_cols.extend(table.get("rows", []))

    # Build column lookup by table ID
    # First build table ID -> name mapping
    table_id_to_name = {}
    for t in raw_tables:
        tid = t.get("[ID]", t.get("ID"))
        name = t.get("[Name]", t.get("Name"))
        if tid is not None and name:
            table_id_to_name[tid] = name

    # Group columns by table name
    columns_by_table = {}
    for col in raw_cols:
        table_id = col.get("[TableID]", col.get("TableID"))
        col_name = col.get("[ExplicitName]", col.get("ExplicitName"))
        is_hidden = col.get("[IsHidden]", col.get("IsHidden", False))

        if table_id is None or not col_name or is_hidden:
            continue

        table_name = table_id_to_name.get(table_id)
        if table_name and table_name in table_names:
            columns_by_table.setdefault(table_name, []).append({
                "name": col_name,
                "dataType": _dax_type_to_powerbi_type(
                    col.get("[ExplicitDataType]", col.get("ExplicitDataType", 0))
                ),
            })

    # Build result in same format as REST /tables endpoint
    tables = []
    for name in table_names:
        tables.append({
            "name": name,
            "columns": columns_by_table.get(name, []),
        })

    return tables


# INFO.COLUMNS() returns numeric type IDs; map to Power BI type names
_DAX_TYPE_MAP = {
    2: "String",
    6: "Int64",
    8: "Double",
    9: "DateTime",
    10: "Decimal",
    11: "Boolean",
}


def _dax_type_to_powerbi_type(type_id) -> str:
    """Convert a DAX ExplicitDataType ID to a Power BI type name."""
    if isinstance(type_id, (int, float)):
        return _DAX_TYPE_MAP.get(int(type_id), "String")
    return "String"


class TapPowerBI(Tap):
    """PowerBI tap class."""

    name = "tap-powerbi"

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("redirect_uri", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Discover all workspaces, datasets, and tables. Create a stream per table."""
        streams: List[Stream] = [
            WorkspacesStream(tap=self),
            DatasetsStream(tap=self),
            DatasetTablesStream(tap=self),
        ]

        token = get_access_token(self.config)
        headers = {"Authorization": f"Bearer {token}"}

        try:
            workspaces_resp = requests.get(f"{API_BASE}/groups", headers=headers)
            workspaces_resp.raise_for_status()
            workspaces = workspaces_resp.json().get("value", [])
        except Exception as e:
            logger.warning(f"Failed to discover workspaces: {e}")
            return streams

        for ws in workspaces:
            ws_id = ws["id"]
            try:
                ds_resp = requests.get(
                    f"{API_BASE}/groups/{ws_id}/datasets", headers=headers
                )
                ds_resp.raise_for_status()
                datasets = ds_resp.json().get("value", [])
            except Exception as e:
                logger.warning(f"Failed to list datasets for workspace {ws_id}: {e}")
                continue

            for ds in datasets:
                ds_id = ds["id"]
                ds_name = ds["name"]

                # Try REST /tables first, fall back to DAX INFO.TABLES()
                tables = None
                try:
                    tables = _discover_tables_via_rest(ws_id, ds_id, headers)
                    logger.info(f"Discovered {len(tables)} tables via REST for dataset {ds_name}")
                except Exception:
                    pass

                if tables is None:
                    try:
                        tables = _discover_tables_via_dax(ws_id, ds_id, headers)
                        logger.info(f"Discovered {len(tables)} tables via DAX for dataset {ds_name}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to discover tables for dataset {ds_name} ({ds_id}): {e}"
                        )
                        continue

                for table in tables:
                    table_name = table["name"]
                    columns = table.get("columns", [])
                    stream_name = f"{ds_name}__{table_name}"
                    streams.append(
                        TableDataStream(
                            tap=self,
                            name=stream_name,
                            workspace_id=ws_id,
                            dataset_id=ds_id,
                            dataset_name=ds_name,
                            table_name=table_name,
                            columns=columns,
                        )
                    )

        return streams


if __name__ == "__main__":
    TapPowerBI.cli()
