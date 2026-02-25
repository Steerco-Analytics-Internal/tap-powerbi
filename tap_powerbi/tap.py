"""PowerBI tap class."""

import logging
import time
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
from tap_powerbi.tmdl_parser import tables_from_definition

logger = logging.getLogger(__name__)

API_BASE = "https://api.powerbi.com/v1.0/myorg"
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


def _discover_tables_via_rest(ws_id, ds_id, headers):
    """Try GET /tables endpoint (works for push datasets)."""
    resp = requests.get(
        f"{API_BASE}/groups/{ws_id}/datasets/{ds_id}/tables",
        headers=headers,
    )
    resp.raise_for_status()
    return resp.json().get("value", [])


def _discover_tables_via_fabric(ws_id, ds_id, headers):
    """Use Fabric getDefinition API to discover tables from TMDL."""
    resp = requests.post(
        f"{FABRIC_API_BASE}/workspaces/{ws_id}/semanticModels/{ds_id}/getDefinition",
        headers={**headers, "Content-Type": "application/json"},
    )

    if resp.status_code == 200:
        definition = resp.json().get("definition", {})
    elif resp.status_code == 202:
        definition = _poll_fabric_operation(resp, headers)
    else:
        resp.raise_for_status()
        return []  # unreachable

    return tables_from_definition(definition)


def _poll_fabric_operation(initial_resp, headers, max_polls=30):
    """Poll a Fabric long-running operation until completion."""
    operation_url = initial_resp.headers.get("Location")
    operation_id = initial_resp.headers.get("x-ms-operation-id")
    retry_after = int(initial_resp.headers.get("Retry-After", 5))

    for _ in range(max_polls):
        time.sleep(retry_after)
        poll_resp = requests.get(operation_url, headers=headers)
        poll_resp.raise_for_status()

        poll_data = poll_resp.json()
        status = poll_data.get("status")

        if status == "Succeeded":
            result_resp = requests.get(
                f"{FABRIC_API_BASE}/operations/{operation_id}/result",
                headers=headers,
            )
            result_resp.raise_for_status()
            return result_resp.json().get("definition", {})

        if status == "Failed":
            error = poll_data.get("error", {})
            raise RuntimeError(
                f"Fabric getDefinition failed: {error.get('message', status)}"
            )

        retry_after = int(poll_resp.headers.get("Retry-After", retry_after))

    raise TimeoutError("Fabric getDefinition polling timed out")


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

        fabric_token = get_access_token(self.config, resource="https://api.fabric.microsoft.com")
        fabric_headers = {"Authorization": f"Bearer {fabric_token}"}

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

                # Try REST /tables first, fall back to Fabric getDefinition
                tables = None
                try:
                    tables = _discover_tables_via_rest(ws_id, ds_id, headers)
                    logger.info(f"Discovered {len(tables)} tables via REST for dataset {ds_name}")
                except Exception:
                    pass

                if tables is None:
                    try:
                        tables = _discover_tables_via_fabric(ws_id, ds_id, fabric_headers)
                        logger.info(f"Discovered {len(tables)} tables via Fabric for dataset {ds_name}")
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
