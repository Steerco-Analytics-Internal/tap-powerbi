"""Stream type classes for tap-powerbi."""


from typing import Any, Optional, Iterable

from singer_sdk import typing as th

from tap_powerbi.client import PowerBIStream
import requests

class ReportsStream(PowerBIStream):
    """Define custom stream."""

    name = "reports"
    path = "/reports"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("datasetId", th.StringType),
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("webUrl", th.StringType),
        th.Property("embedUrl", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "dataset_id": record["datasetId"],
        }


class DataSetsStream(PowerBIStream):
    """Define custom stream."""

    name = "datasets"
    path = "/datasets"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.value[*]"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("webUrl", th.StringType),
        th.Property("configuredBy", th.StringType),
        th.Property("isRefreshable", th.BooleanType),
        th.Property("isEffectiveIdentityRequired", th.BooleanType),
        th.Property("isEffectiveIdentityRolesRequired", th.BooleanType),
        th.Property("isOnPremGatewayRequired", th.BooleanType),
        th.Property("targetStorageMode", th.StringType),
        th.Property("createReportEmbedURL", th.StringType),
        th.Property("qnaEmbedURL", th.StringType),
        th.Property("upstreamDatasets", th.CustomType({"type": ["array", "string"]})),
        th.Property("users", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "dataset_id": record["id"],
            "dataset_name": record["name"],
        }

class ReportDataSetsStream(DataSetsStream):
    name = "report_datasets"
    path = "/datasets/{dataset_id}"
    parent_stream_type = ReportsStream
    records_jsonpath = "$[*]"
    
class DataSetDataStream(PowerBIStream):
    """Define custom stream."""

    name = "dataset_data"
    path = "/datasets/{dataset_id}/executeQueries"
    rest_method = "POST"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.results.[*].tables.[*]"
    parent_stream_type = DataSetsStream
    current_table = None

    schema = th.PropertiesList(
        th.Property("datasetId", th.StringType),
        th.Property("dataset_name", th.StringType),
        th.Property("rows", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()
    
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """
        Override the get_records function so we could yield all of sellingProgram type report for each time period
        """
        for table in self.get_tables(context):
            self.current_table = table
            yield from super().get_records(context)
    
    def get_dataset_tables(self, context: Optional[dict]) -> list:
        url = self.url_base + f"/datasets/{context.get('dataset_id')}/tables"
        headers = self.authenticator.auth_headers
        resp =  requests.get(url, headers=headers)
        tables = []
        if resp.status_code == 400:
            self.logger.warn(f"Failed to get tables for dataset {context.get('dataset_id')}. Response: {resp.text}")
        if resp.status_code == 200 and "value" in resp.json():    
            for table in resp.json()['value']:
                tables.append(table['name'])    
        return tables
        
    def get_tables(self, context: Optional[dict]) -> list:
        if self.config.get("tables"):
            return self.config.get("tables")
        else:
            return self.get_dataset_tables(context)
        

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        #Query limitation - https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries#datasetexecutequeriesquery
        #DAX Studio limitation for TopnSkip with Power BI https://github.com/DaxStudio/DaxStudio/issues/472
        query = {
            "queries": [
                {
                    # "query": f"EVALUATE TopnSkip({self._page_size},{self.offset},'{self.current_table}')"
                    #Get maximum rows without paginating
                    "query": f"EVALUATE Values('{self.current_table}')"
                }
            ],
            "serializerSettings": {"includeNulls": True},
        }
        return query

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["datasetId"] = context.get("dataset_id")
        row["dataset_name"] = context.get("dataset_name")
        return row
    
    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        #Disable pagination, need to figure different pagination logic for different dataset types
        return None

class ReportDataSetDataStream(PowerBIStream):
    """Define custom stream."""

    name = "report_dataset_data"
    path = "/datasets/{dataset_id}/executeQueries"
    rest_method = "POST"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.results.[*].tables.[*]"
    parent_stream_type = ReportDataSetsStream
    
    schema = th.PropertiesList(
        th.Property("datasetId", th.StringType),
        th.Property("dataset_name", th.StringType),
        th.Property("rows", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()
    
    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        query = {
            "queries": [
                {
                    "query": f"EVALUATE TopnSkip({self._page_size},{self.offset},'{context.get('dataset_name')}')"
                }
            ],
            "serializerSettings": {"includeNulls": True},
        }
        return query
    
    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["datasetId"] = context.get("dataset_id")
        row["dataset_name"] = context.get("dataset_name")
        return row

