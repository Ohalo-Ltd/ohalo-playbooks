"""
Test the parsing and validation of the proxy configuration.
"""

import pytest

from smtp_proxy.config import Config


@pytest.fixture
def required_env(monkeypatch):
    """Set the required settings, so that validate() checks only the setting under test."""
    monkeypatch.setenv("DATAXRAY_BASE_URL", "https://dataxray.example.com/api")
    monkeypatch.setenv("DATAXRAY_API_KEY", "test-api-key")
    monkeypatch.setenv("DATASOURCE_ID", "100")
    monkeypatch.delenv("DATAXRAY_VERIFY_TLS", raising=False)


class TestConfig:
    def test_verify_tls_is_true_when_unset(self, required_env):
        assert Config().dataxray_verify_tls is True

    def test_validate_accepts_config_when_settings_are_unset(self, required_env):
        assert Config().validate() is None

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("true", True),
            ("TRUE", True),
            ("false", False),
            (" False ", False),
        ],
    )
    def test_parses_verify_tls_when_value_is_true_or_false(
        self, required_env, monkeypatch, value, expected
    ):
        monkeypatch.setenv("DATAXRAY_VERIFY_TLS", value)

        config = Config()

        assert config.dataxray_verify_tls == expected

    @pytest.mark.parametrize("value", ["true", "False"])
    def test_validate_accepts_verify_tls_when_value_is_true_or_false(
        self, required_env, monkeypatch, value
    ):
        monkeypatch.setenv("DATAXRAY_VERIFY_TLS", value)

        assert Config().validate() is None

    @pytest.mark.parametrize("value", ["yes", "", "0"])
    def test_validate_rejects_verify_tls_when_value_is_not_true_or_false(
        self, required_env, monkeypatch, value
    ):
        monkeypatch.setenv("DATAXRAY_VERIFY_TLS", value)

        assert Config().validate() == "DATAXRAY_VERIFY_TLS must be true or false"

    @pytest.mark.parametrize(
        "value",
        [
            "https://dataxray.example.com",
            "https://dataxray.example.com/",
            "https://dataxray.example.com/api",
            "https://dataxray.example.com/api/",
        ],
    )
    def test_base_url_ends_in_api_when_value_has_or_lacks_api(
        self, required_env, monkeypatch, value
    ):
        monkeypatch.setenv("DATAXRAY_BASE_URL", value)

        assert Config().dataxray_base_url == "https://dataxray.example.com/api"

    def test_validate_rejects_missing_base_url_when_unset(
        self, required_env, monkeypatch
    ):
        monkeypatch.delenv("DATAXRAY_BASE_URL")

        assert (
            Config().validate() == "DATAXRAY_BASE_URL environment variable is required"
        )
