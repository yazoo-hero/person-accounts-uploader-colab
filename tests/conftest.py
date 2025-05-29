"""Pytest configuration and fixtures."""

# Add src to Python path
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def sample_workday_data() -> pd.DataFrame:
    """Create sample Workday data for testing."""
    return pd.DataFrame(
        {
            "WiserId": ["12345", "67890", "11111"],
            "AbsenceType": ["Annual Leave", "Sick Leave", "Annual Leave"],
            "Beginning Year Balance": [10.0, 5.0, 15.0],
            "Accrued this year": [12.0, 0.0, 8.0],
            "Units Approved": [5.0, 2.0, 10.0],
            "StartDate": ["2024-01-01", "2024-02-01", "2024-01-15"],
        }
    )


@pytest.fixture
def sample_calabrio_data() -> pd.DataFrame:
    """Create sample Calabrio data for testing."""
    return pd.DataFrame(
        {
            "EmploymentNumber": ["12345", "67890", "22222"],
            "AbsenceName": ["annual leave", "sick leave", "annual leave"],
            "BalanceIn": [10.0, 5.0, 20.0],
            "Accrued": [12.0, 0.0, 10.0],
            "Extra": [0.0, 0.0, 2.0],
            "BusinessUnitName": ["Unit A", "Unit B", "Unit A"],
            "PersonId": ["P001", "P002", "P003"],
            "ContractName": ["Full Time", "Full Time", "Part Time"],
        }
    )


@pytest.fixture
def sample_people_data() -> pd.DataFrame:
    """Create sample people data for testing."""
    return pd.DataFrame(
        {
            "WiserId": ["12345", "67890", "11111"],
            "Latest Headcount Hire Date": ["2020-01-15", "2019-06-01", "2021-03-20"],
            "Latest Headcount Primary Work Email": [
                "john.doe@example.com",
                "jane.smith@example.com",
                "bob.jones@example.com",
            ],
        }
    )


@pytest.fixture
def sample_config_data() -> Dict[str, Any]:
    """Create sample configuration data for testing."""
    return {
        "Unit A": {
            "absences": {
                "Result": [
                    {"Id": "ABS001", "Name": "Annual Leave"},
                    {"Id": "ABS002", "Name": "Sick Leave"},
                ]
            }
        },
        "Unit B": {
            "absences": {
                "Result": [
                    {"Id": "ABS003", "Name": "Annual Leave"},
                    {"Id": "ABS004", "Name": "Sick Leave"},
                ]
            }
        },
    }


@pytest.fixture
def balance_rules() -> Dict[str, Any]:
    """Create sample balance rules for testing."""
    return {
        "default_values": {"beginning_year_balance": 0, "accrued_this_year": 0},
        "absence_rules": {
            "Annual Leave": {
                "calculation_method": "standard",
                "max_carryover": 40,
                "accrual_rate": 1.67,
            },
            "Sick Leave": {"calculation_method": "fixed", "fixed_balance": 5, "accrual_rate": 0},
        },
    }


@pytest.fixture
def temp_data_dir(tmp_path) -> Path:
    """Create a temporary data directory structure."""
    data_dir = tmp_path / "data"
    workday_dir = data_dir / "workday"
    calabrio_dir = data_dir / "calabrio"

    # Create directories
    workday_dir.mkdir(parents=True)
    calabrio_dir.mkdir(parents=True)

    return data_dir


@pytest.fixture
def mock_api_response() -> Dict[str, Any]:
    """Create a mock API response."""
    return {
        "success": True,
        "data": {"id": "123", "status": "uploaded", "timestamp": datetime.now().isoformat()},
        "error": None,
    }


@pytest.fixture
def env_setup(monkeypatch):
    """Set up environment variables for testing."""
    monkeypatch.setenv("CALABRIO_API_BASE_URL", "https://test.calabrio.com")
    monkeypatch.setenv("CALABRIO_API_KEY", "test-api-key")
    monkeypatch.setenv("CALABRIO_API_SECRET", "test-api-secret")
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
