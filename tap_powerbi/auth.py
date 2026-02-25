"""PowerBI Authentication."""

import requests

from singer import utils
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


def get_access_token(config: dict, resource: str = "https://analysis.windows.net/powerbi/api") -> str:
    """Get an access token using the refresh token grant. Standalone (no stream needed)."""
    response = requests.post(
        "https://login.microsoftonline.com/common/oauth2/token",
        data={
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "redirect_uri": config["redirect_uri"],
            "refresh_token": config["refresh_token"],
            "grant_type": "refresh_token",
            "resource": resource,
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]


class PowerBIAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for PowerBI."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the PowerBI API."""
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "redirect_uri": self.config["redirect_uri"],
            "refresh_token": self.config["refresh_token"],
            "grant_type": "refresh_token",
        }

    @classmethod
    def create_for_stream(cls, stream) -> "PowerBIAuthenticator":
        return cls(
            stream=stream,
            auth_endpoint=f"https://login.microsoftonline.com/common/oauth2/token",
        )

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        if int(self.expires_in) > (utils.now() - self.last_refreshed).total_seconds():
            return True
        return False
