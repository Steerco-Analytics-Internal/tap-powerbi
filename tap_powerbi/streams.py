"""Stream type classes for tap-powerbi."""

from typing import Any, Optional

from singer_sdk import typing as th

from tap_powerbi.client import PowerBIStream
from tap_powerbi.type_mapping import build_schema_from_columns
from tap_powerbi.row_flattener import flatten_row


class WorkspacesStream(PowerBIStream):
    """Lists all Power BI workspaces the authenticated user can access."""

    name = "workspaces"
    path = "/groups"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("isReadOnly", th.BooleanType),
        th.Property("isOnDedicatedCapacity", th.BooleanType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "workspace_id": record["id"],
            "workspace_name": record["name"],
        }


class DatasetsStream(PowerBIStream):
    """Lists datasets (semantic models) in a workspace."""

    name = "datasets"
    path = "/groups/{workspace_id}/datasets"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = WorkspacesStream

    schema = th.PropertiesList(
        th.Property("workspace_id", th.StringType),
        th.Property("workspace_name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("configuredBy", th.StringType),
        th.Property("isRefreshable", th.BooleanType),
        th.Property("webUrl", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["workspace_id"] = context.get("workspace_id")
        row["workspace_name"] = context.get("workspace_name")
        return row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "workspace_id": context.get("workspace_id") if context else record.get("workspace_id"),
            "dataset_id": record["id"],
            "dataset_name": record["name"],
        }


class DatasetTablesStream(PowerBIStream):
    """Lists tables and their columns within a dataset (semantic model)."""

    name = "dataset_tables"
    path = "/groups/{workspace_id}/datasets/{dataset_id}/tables"
    primary_keys = ["dataset_id", "table_name"]
    replication_key = None
    parent_stream_type = DatasetsStream

    schema = th.PropertiesList(
        th.Property("workspace_id", th.StringType),
        th.Property("dataset_id", th.StringType),
        th.Property("dataset_name", th.StringType),
        th.Property("table_name", th.StringType),
        th.Property("columns", th.CustomType({"type": ["array", "null"]})),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        return {
            "workspace_id": context.get("workspace_id"),
            "dataset_id": context.get("dataset_id"),
            "dataset_name": context.get("dataset_name"),
            "table_name": row.get("name"),
            "columns": row.get("columns", []),
        }

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "workspace_id": context.get("workspace_id") if context else record.get("workspace_id"),
            "dataset_id": context.get("dataset_id") if context else record.get("dataset_id"),
            "dataset_name": context.get("dataset_name") if context else record.get("dataset_name"),
            "table_name": record["table_name"],
            "table_columns": record.get("columns", []),
        }


class TableDataStream(PowerBIStream):
    """Extracts row data from a single table in a dataset via DAX query.

    Instances are created dynamically during discover_streams() -- one per table.
    """

    rest_method = "POST"
    records_jsonpath = "$.results[*].tables[*].rows[*]"

    def __init__(self, tap, name: str, workspace_id: str, dataset_id: str,
                 dataset_name: str, table_name: str, columns: list, **kwargs):
        self._workspace_id = workspace_id
        self._dataset_id = dataset_id
        self._dataset_name = dataset_name
        self._table_name = table_name
        self._columns = columns
        super().__init__(tap=tap, name=name, schema=self._build_schema())

    def _build_schema(self) -> dict:
        return build_schema_from_columns(self._columns)

    @property
    def path(self) -> str:
        return f"/groups/{self._workspace_id}/datasets/{self._dataset_id}/executeQueries"

    def prepare_request_payload(
        self, context: Optional[Any], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        return {
            "queries": [{"query": f"EVALUATE VALUES('{self._table_name}')"}],
            "serializerSettings": {"includeNulls": True},
        }

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        return flatten_row(row)

    def get_next_page_token(
        self, response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        return None
