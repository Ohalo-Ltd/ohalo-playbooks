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
    monkeypatch.delenv("FAIL_OPEN", raising=False)
    monkeypatch.delenv("DATAXRAY_VERIFY_TLS", raising=False)


class TestConfig:
    def test_fail_open_is_false_when_unset(self, required_env):
        assert Config().fail_open is False

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
    def test_parses_fail_open_when_value_is_true_or_false(
        self, required_env, monkeypatch, value, expected
    ):
        monkeypatch.setenv("FAIL_OPEN", value)

        config = Config()

        assert config.fail_open == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("true", True),
            ("false", False),
            ("FALSE", False),
        ],
    )
    def test_parses_verify_tls_when_value_is_true_or_false(
        self, required_env, monkeypatch, value, expected
    ):
        monkeypatch.setenv("DATAXRAY_VERIFY_TLS", value)

        config = Config()

        assert config.dataxray_verify_tls == expected

    @pytest.mark.parametrize(
        "name, value",
        [
            ("FAIL_OPEN", "true"),
            ("FAIL_OPEN", "FALSE"),
            ("DATAXRAY_VERIFY_TLS", "true"),
            ("DATAXRAY_VERIFY_TLS", "False"),
        ],
    )
    def test_validate_accepts_setting_when_value_is_true_or_false(
        self, required_env, monkeypatch, name, value
    ):
        monkeypatch.setenv(name, value)

        assert Config().validate() is None

    @pytest.mark.parametrize(
        "name, value, expected_error",
        [
            ("FAIL_OPEN", "yes", "FAIL_OPEN must be true or false"),
            ("FAIL_OPEN", "", "FAIL_OPEN must be true or false"),
            ("DATAXRAY_VERIFY_TLS", "0", "DATAXRAY_VERIFY_TLS must be true or false"),
        ],
    )
    def test_validate_rejects_setting_when_value_is_not_true_or_false(
        self, required_env, monkeypatch, name, value, expected_error
    ):
        monkeypatch.setenv(name, value)

        assert Config().validate() == expected_error

    def test_validate_rejects_missing_base_url_when_unset(
        self, required_env, monkeypatch
    ):
        monkeypatch.delenv("DATAXRAY_BASE_URL")

        assert (
            Config().validate() == "DATAXRAY_BASE_URL environment variable is required"
        )
