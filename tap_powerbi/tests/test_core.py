"""Core tap configuration tests."""

from tap_powerbi.tap import TapPowerBI


def test_config_schema_requires_auth_fields():
    schema = TapPowerBI.config_jsonschema
    required = schema.get("required", [])
    assert "client_id" in required
    assert "client_secret" in required
    assert "redirect_uri" in required
    assert "refresh_token" in required
