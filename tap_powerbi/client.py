"""REST client handling, including PowerBIStream base class."""

from typing import Any, Dict, Optional

import requests
from memoization import cached
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_powerbi.auth import PowerBIAuthenticator


class PowerBIStream(RESTStream):
    """PowerBI stream class."""

    url_base = "https://api.powerbi.com/v1.0/myorg"
    records_jsonpath = "$.value[*]"

    @property
    @cached
    def authenticator(self) -> PowerBIAuthenticator:
        """Return a new authenticator object."""
        return PowerBIAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """No pagination by default."""
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        return {}

    def validate_response(self, response: requests.Response) -> None:
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)
