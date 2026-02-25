"""Tests for auth helpers."""

from unittest.mock import patch, MagicMock
from tap_powerbi.auth import get_access_token


def test_get_access_token():
    config = {
        "client_id": "test-client",
        "client_secret": "test-secret",
        "redirect_uri": "http://localhost",
        "refresh_token": "test-refresh",
    }
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "fresh-token"}
    mock_response.raise_for_status = MagicMock()

    with patch("tap_powerbi.auth.requests.post", return_value=mock_response) as mock_post:
        token = get_access_token(config)

    assert token == "fresh-token"
    mock_post.assert_called_once_with(
        "https://login.microsoftonline.com/common/oauth2/token",
        data={
            "client_id": "test-client",
            "client_secret": "test-secret",
            "redirect_uri": "http://localhost",
            "refresh_token": "test-refresh",
            "grant_type": "refresh_token",
        },
    )
