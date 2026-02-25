"""Tests for stream definitions."""

from tap_powerbi.streams import WorkspacesStream, DatasetsStream, DatasetTablesStream, TableDataStream


# --- WorkspacesStream ---

def test_workspaces_stream_attributes():
    assert WorkspacesStream.name == "workspaces"
    assert WorkspacesStream.path == "/groups"
    assert WorkspacesStream.primary_keys == ["id"]
    assert "id" in WorkspacesStream.schema["properties"]
    assert "name" in WorkspacesStream.schema["properties"]


def test_workspaces_child_context():
    stream = WorkspacesStream.__new__(WorkspacesStream)
    record = {"id": "ws-123", "name": "Team Alpha"}
    ctx = stream.get_child_context(record, None)
    assert ctx == {"workspace_id": "ws-123", "workspace_name": "Team Alpha"}


# --- DatasetsStream ---

def test_datasets_stream_attributes():
    assert DatasetsStream.name == "datasets"
    assert DatasetsStream.path == "/groups/{workspace_id}/datasets"
    assert DatasetsStream.primary_keys == ["id"]
    assert DatasetsStream.parent_stream_type is WorkspacesStream
    assert "workspace_id" in DatasetsStream.schema["properties"]
    assert "name" in DatasetsStream.schema["properties"]


def test_datasets_child_context():
    stream = DatasetsStream.__new__(DatasetsStream)
    record = {"id": "ds-456", "name": "TestModel"}
    context = {"workspace_id": "ws-123", "workspace_name": "Team Alpha"}
    ctx = stream.get_child_context(record, context)
    assert ctx == {
        "workspace_id": "ws-123",
        "dataset_id": "ds-456",
        "dataset_name": "TestModel",
    }


# --- DatasetTablesStream ---

def test_dataset_tables_stream_attributes():
    assert DatasetTablesStream.name == "dataset_tables"
    assert DatasetTablesStream.path == "/groups/{workspace_id}/datasets/{dataset_id}/tables"
    assert DatasetTablesStream.parent_stream_type is DatasetsStream
    assert "table_name" in DatasetTablesStream.schema["properties"]
    assert "columns" in DatasetTablesStream.schema["properties"]


def test_dataset_tables_child_context():
    stream = DatasetTablesStream.__new__(DatasetTablesStream)
    record = {
        "table_name": "Items",
        "columns": [
            {"name": "Label", "dataType": "String"},
            {"name": "Value", "dataType": "Double"},
        ],
    }
    context = {"workspace_id": "ws-1", "dataset_id": "ds-1", "dataset_name": "TestModel"}
    ctx = stream.get_child_context(record, context)
    assert ctx["workspace_id"] == "ws-1"
    assert ctx["dataset_id"] == "ds-1"
    assert ctx["dataset_name"] == "TestModel"
    assert ctx["table_name"] == "Items"
    assert ctx["table_columns"] == record["columns"]


# --- TableDataStream ---

def test_table_data_stream_schema():
    stream = TableDataStream.__new__(TableDataStream)
    stream._columns = [
        {"name": "Label", "dataType": "String"},
        {"name": "Value", "dataType": "Double"},
    ]
    schema = stream._build_schema()
    assert "Label" in schema["properties"]
    assert schema["properties"]["Label"] == {"type": ["string", "null"]}
    assert schema["properties"]["Value"] == {"type": ["number", "null"]}


def test_table_data_stream_path():
    stream = TableDataStream.__new__(TableDataStream)
    stream._workspace_id = "ws-1"
    stream._dataset_id = "ds-1"
    assert stream.path == "/groups/ws-1/datasets/ds-1/executeQueries"


def test_table_data_stream_payload():
    stream = TableDataStream.__new__(TableDataStream)
    stream._table_name = "Items"
    payload = stream.prepare_request_payload(context=None, next_page_token=None)
    assert payload == {
        "queries": [{"query": "EVALUATE VALUES('Items')"}],
        "serializerSettings": {"includeNulls": True},
    }


def test_table_data_stream_post_process():
    stream = TableDataStream.__new__(TableDataStream)
    raw_row = {"[Items].[Label]": "foo", "[Items].[Value]": 42}
    result = stream.post_process(raw_row, context=None)
    assert result == {"Label": "foo", "Value": 42}


from unittest.mock import patch, MagicMock
from tap_powerbi.tap import TapPowerBI

SAMPLE_CONFIG = {
    "client_id": "test",
    "client_secret": "test",
    "redirect_uri": "http://localhost",
    "refresh_token": "test",
}


def mock_api_responses(*args, **kwargs):
    """Return mock API responses based on URL (REST /tables works)."""
    url = args[0] if args else kwargs.get("url", "")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    if "/groups" in url and "/datasets" not in url:
        mock_resp.json.return_value = {
            "value": [{"id": "ws-1", "name": "TestWorkspace"}]
        }
    elif "/datasets" in url and "/tables" not in url:
        mock_resp.json.return_value = {
            "value": [{"id": "ds-1", "name": "TestModel"}]
        }
    elif "/tables" in url:
        mock_resp.json.return_value = {
            "value": [
                {
                    "name": "Items",
                    "columns": [
                        {"name": "Label", "dataType": "String"},
                        {"name": "Value", "dataType": "Double"},
                    ],
                }
            ]
        }
    return mock_resp


def _mock_auth_post():
    return MagicMock(
        json=MagicMock(return_value={"access_token": "tok"}),
        raise_for_status=MagicMock(),
    )


@patch("tap_powerbi.auth.requests.post")
@patch("tap_powerbi.tap.requests.get", side_effect=mock_api_responses)
def test_discover_streams_creates_dynamic_table_streams(mock_get, mock_auth_post):
    mock_auth_post.return_value = _mock_auth_post()
    tap = TapPowerBI(config=SAMPLE_CONFIG, parse_env_config=False)
    streams = tap.discover_streams()

    stream_names = [s.name for s in streams]
    assert "workspaces" in stream_names
    assert "datasets" in stream_names
    assert "dataset_tables" in stream_names
    assert "TestModel__Items" in stream_names

    table_stream = next(s for s in streams if s.name == "TestModel__Items")
    assert "Label" in table_stream.schema["properties"]
    assert "Value" in table_stream.schema["properties"]


@patch("tap_powerbi.auth.requests.post")
@patch("tap_powerbi.tap.requests.get")
def test_discover_streams_handles_api_errors_gracefully(mock_get, mock_auth_post):
    """If workspace listing fails, we still get the 3 base streams."""
    mock_auth_post.return_value = _mock_auth_post()
    mock_get.side_effect = Exception("Network error")

    tap = TapPowerBI(config=SAMPLE_CONFIG, parse_env_config=False)
    streams = tap.discover_streams()

    stream_names = [s.name for s in streams]
    assert "workspaces" in stream_names
    assert "datasets" in stream_names
    assert "dataset_tables" in stream_names
    assert len(streams) == 3  # no dynamic streams if discovery failed


import base64


def _encode_tmdl(text):
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


_ITEMS_TMDL = """\
table Items

    column Label
        dataType: string
        sourceColumn: Label

    column Value
        dataType: double
        sourceColumn: Value
"""

_HIDDEN_TMDL = """\
table InternalCalc
    isHidden

    column Temp
        dataType: string
"""


def _mock_fabric_fallback_get(*args, **kwargs):
    """Simulate REST /tables returning 403, forcing Fabric fallback."""
    url = args[0] if args else kwargs.get("url", "")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    if "/groups" in url and "/datasets" not in url:
        mock_resp.json.return_value = {
            "value": [{"id": "ws-1", "name": "TestWorkspace"}]
        }
    elif "/datasets" in url and "/tables" not in url:
        mock_resp.json.return_value = {
            "value": [{"id": "ds-1", "name": "TestModel"}]
        }
    elif "/tables" in url:
        from requests.exceptions import HTTPError
        mock_resp.status_code = 403
        mock_resp.raise_for_status.side_effect = HTTPError("403 Forbidden")
    return mock_resp


def _mock_fabric_fallback_post(*args, **kwargs):
    """Mock for requests.post — handles auth and Fabric getDefinition."""
    url = args[0] if args else kwargs.get("url", "")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    if "login.microsoftonline.com" in url:
        mock_resp.json.return_value = {"access_token": "tok"}
    elif "/getDefinition" in url:
        mock_resp.json.return_value = {
            "definition": {
                "parts": [
                    {
                        "path": "definition/tables/Items.tmdl",
                        "payload": _encode_tmdl(_ITEMS_TMDL),
                        "payloadType": "InlineBase64",
                    },
                    {
                        "path": "definition/tables/InternalCalc.tmdl",
                        "payload": _encode_tmdl(_HIDDEN_TMDL),
                        "payloadType": "InlineBase64",
                    },
                ]
            }
        }
    return mock_resp


@patch("tap_powerbi.tap.requests.post", side_effect=_mock_fabric_fallback_post)
@patch("tap_powerbi.tap.requests.get", side_effect=_mock_fabric_fallback_get)
@patch("tap_powerbi.auth.requests.post", side_effect=_mock_fabric_fallback_post)
def test_discover_streams_falls_back_to_fabric(mock_auth_post, mock_get, mock_tap_post):
    """When REST /tables returns 403, fall back to Fabric getDefinition."""
    tap = TapPowerBI(config=SAMPLE_CONFIG, parse_env_config=False)
    streams = tap.discover_streams()

    stream_names = [s.name for s in streams]
    assert "TestModel__Items" in stream_names
    # Hidden table should be excluded
    assert "TestModel__InternalCalc" not in stream_names

    table_stream = next(s for s in streams if s.name == "TestModel__Items")
    assert "Label" in table_stream.schema["properties"]
    assert "Value" in table_stream.schema["properties"]
