"""Unit tests for configuration management."""

from pathlib import Path

import pytest

from src.utils.config import Config
from src.utils.exceptions import ConfigurationError


class TestConfig:
    """Test cases for Config class."""

    def test_config_initialization(self, env_setup):
        """Test config initialization with environment variables."""
        config = Config()

        assert config.CALABRIO_API_BASE_URL == "https://test.calabrio.com"
        assert config.CALABRIO_API_KEY == "test-api-key"
        assert config.CALABRIO_API_SECRET == "test-api-secret"
        assert config.APP_ENV == "test"
        assert config.LOG_LEVEL == "DEBUG"

    def test_default_values(self, monkeypatch):
        """Test config with default values."""
        # Clear environment variables
        for key in ["CALABRIO_API_BASE_URL", "CALABRIO_API_KEY", "APP_ENV"]:
            monkeypatch.delenv(key, raising=False)

        config = Config()

        assert config.CALABRIO_API_BASE_URL == ""
        assert config.APP_ENV == "development"
        assert config.APP_DEBUG is False
        assert config.APP_PORT == 8050

    def test_production_validation_fails(self, monkeypatch):
        """Test that production environment requires API credentials."""
        monkeypatch.setenv("APP_ENV", "production")
        monkeypatch.delenv("CALABRIO_API_BASE_URL", raising=False)

        with pytest.raises(ConfigurationError) as exc_info:
            Config()

        assert "CALABRIO_API_BASE_URL is required" in str(exc_info.value)

    def test_path_configuration(self, env_setup):
        """Test path configuration."""
        config = Config()

        assert isinstance(config.BASE_DIR, Path)
        assert isinstance(config.DATA_DIR, Path)
        assert isinstance(config.CONFIG_DIR, Path)
        assert config.WORKDAY_DIR == config.DATA_DIR / "workday"
        assert config.CALABRIO_DIR == config.DATA_DIR / "calabrio"

    def test_load_json_config_success(self, tmp_path, env_setup):
        """Test successful JSON config loading."""
        # Create a test JSON file
        test_file = tmp_path / "test_config.json"
        test_data = {"key": "value", "number": 42}
        test_file.write_text('{"key": "value", "number": 42}')

        config = Config()
        result = config.load_json_config(test_file)

        assert result == test_data

    def test_load_json_config_file_not_found(self, env_setup):
        """Test loading non-existent JSON file."""
        config = Config()
        result = config.load_json_config(Path("nonexistent.json"))

        assert result == {}

    def test_load_json_config_invalid_json(self, tmp_path, env_setup):
        """Test loading invalid JSON file."""
        test_file = tmp_path / "invalid.json"
        test_file.write_text("{ invalid json }")

        config = Config()

        with pytest.raises(ConfigurationError) as exc_info:
            config.load_json_config(test_file)

        assert "Invalid JSON" in str(exc_info.value)

    def test_debug_mode(self, monkeypatch):
        """Test debug mode configuration."""
        monkeypatch.setenv("APP_DEBUG", "true")
        config = Config()
        assert config.APP_DEBUG is True

        monkeypatch.setenv("APP_DEBUG", "false")
        config = Config()
        assert config.APP_DEBUG is False

        monkeypatch.setenv("APP_DEBUG", "TRUE")
        config = Config()
        assert config.APP_DEBUG is True

    def test_custom_paths(self, monkeypatch, tmp_path):
        """Test custom path configuration."""
        custom_data = tmp_path / "custom_data"
        custom_config = tmp_path / "custom_config"

        monkeypatch.setenv("DATA_DIR", str(custom_data))
        monkeypatch.setenv("CONFIG_DIR", str(custom_config))

        config = Config()

        assert config.DATA_DIR == custom_data
        assert config.CONFIG_DIR == custom_config
