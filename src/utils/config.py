"""Configuration management for the application."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from ..utils.exceptions import ConfigurationError

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class Config:
    """Application configuration."""

    def __init__(self):
        # API Configuration
        self.CALABRIO_API_BASE_URL = os.getenv("CALABRIO_API_BASE_URL", "")
        self.CALABRIO_API_KEY = os.getenv("CALABRIO_API_KEY", "")
        self.CALABRIO_API_SECRET = os.getenv("CALABRIO_API_SECRET", "")

        # Application Settings
        self.APP_ENV = os.getenv("APP_ENV", "development")
        self.APP_DEBUG = os.getenv("APP_DEBUG", "false").lower() == "true"
        self.APP_PORT = int(os.getenv("APP_PORT", "8050"))

        # Paths
        self.BASE_DIR = Path(__file__).parent.parent.parent
        self.DATA_DIR = Path(os.getenv("DATA_DIR", self.BASE_DIR / "data"))
        self.CONFIG_DIR = Path(os.getenv("CONFIG_DIR", self.BASE_DIR / "config"))

        # Data subdirectories
        self.WORKDAY_DIR = self.DATA_DIR / "workday"
        self.CALABRIO_DIR = self.DATA_DIR / "calabrio"
        self.PERSON_ACCOUNTS_DIR = self.WORKDAY_DIR / "person_accounts"
        self.PEOPLE_DIR = self.WORKDAY_DIR / "people"
        self.USED_ENTRIES_DIR = self.WORKDAY_DIR / "used_entries"

        # Calabrio data files
        self.ACCOUNT_DATA_PATH = self.CALABRIO_DIR / "account_data.json"
        self.PERSON_DATA_PATH = self.CALABRIO_DIR / "person_data.json"
        self.CONFIG_DATA_PATH = self.CALABRIO_DIR / "config_data.json"

        # Config files
        self.BALANCE_RULES_PATH = self.CONFIG_DIR / "balance_rules.json"
        self.CONTRACT_MAPPER_PATH = self.CONFIG_DIR / "contract_mapper.json"
        self.TIMEZONE_MAPPER_PATH = self.CONFIG_DIR / "timezone_mapper.json"

        # Logging
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_FILE = os.getenv("LOG_FILE", "app.log")

        # Jupyter/Colab
        self.JUPYTER_DASH_MODE = os.getenv("JUPYTER_DASH_MODE", "inline")

        # Validate configuration
        self._validate()

    def _validate(self):
        """Validate configuration settings."""
        if self.APP_ENV == "production":
            if not self.CALABRIO_API_BASE_URL:
                raise ConfigurationError("CALABRIO_API_BASE_URL is required in production")
            if not self.CALABRIO_API_KEY:
                raise ConfigurationError("CALABRIO_API_KEY is required in production")
            if not self.CALABRIO_API_SECRET:
                raise ConfigurationError("CALABRIO_API_SECRET is required in production")

    def load_json_config(self, file_path: Path) -> Dict[str, Any]:
        """Load JSON configuration file."""
        try:
            if not file_path.exists():
                logger.warning(f"Configuration file not found: {file_path}")
                return {}

            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON in {file_path}: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading {file_path}: {e}")

    def get_balance_rules(self) -> Dict[str, Any]:
        """Get balance calculation rules."""
        return self.load_json_config(self.BALANCE_RULES_PATH)

    def get_contract_mapper(self) -> Dict[str, str]:
        """Get contract mapping configuration."""
        return self.load_json_config(self.CONTRACT_MAPPER_PATH)

    def get_timezone_mapper(self) -> Dict[str, str]:
        """Get timezone mapping configuration."""
        return self.load_json_config(self.TIMEZONE_MAPPER_PATH)

    def get_calabrio_config(self) -> Dict[str, Any]:
        """Get Calabrio configuration data."""
        return self.load_json_config(self.CONFIG_DATA_PATH)

    def setup_logging(self):
        """Set up logging configuration."""
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, self.LOG_LEVEL.upper()),
            format=log_format,
            handlers=[logging.FileHandler(self.LOG_FILE), logging.StreamHandler()],
        )

        # Suppress noisy loggers
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)


# Global config instance
config = Config()
