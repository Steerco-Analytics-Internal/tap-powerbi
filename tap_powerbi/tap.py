"""PowerBI tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_powerbi.streams import (
    DataSetDataStream,
    DataSetsStream,
    PowerBIStream,
    ReportsStream,
    ReportDataSetsStream,
    ReportDataSetDataStream,
)

STREAM_TYPES = [
    ReportsStream,
    DataSetsStream,
    DataSetDataStream,
    ReportDataSetsStream,
    ReportDataSetDataStream,
]


class TapPowerBI(Tap):
    """PowerBI tap class."""

    name = "tap-powerbi"

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("redirect_uri", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("tables", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapPowerBI.cli()
