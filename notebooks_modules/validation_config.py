"""Configuration settings for validation."""

from pathlib import Path

# Base directories
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
CONFIG_DIR = PROJECT_ROOT / 'config'  # Added: Configuration directory
LOGS_DIR = PROJECT_ROOT / 'logs'

# Workday data paths
WORKDAY_DIR = DATA_DIR / 'workday'
PERSON_ACCOUNTS_DIR = WORKDAY_DIR / 'person_accounts'
PEOPLE_DIR = WORKDAY_DIR / 'people'
USED_ENTRIES_DIR = WORKDAY_DIR / 'used_entries'

# Calabrio data paths
CALABRIO_DIR = DATA_DIR / 'calabrio'
ACCOUNT_DATA_PATH = CALABRIO_DIR / 'account_data.json'
PERSON_DATA_PATH = CALABRIO_DIR / 'person_data.json'

# Configuration file paths
CONFIG_DATA_PATH = CALABRIO_DIR / 'config_data.json'
BALANCE_RULES_PATH = CONFIG_DIR / 'balance_rules.json'  # Added: Balance rules path

# Log file settings
UPLOAD_LOG_DIR = LOGS_DIR / 'uploads'

# Create each directory
def ensure_directories():
    """Create necessary directories."""
    # Creating base directories
    for directory in [DATA_DIR, CONFIG_DIR, LOGS_DIR]:
        try:
            if directory.exists() and directory.is_file():
                print(f"Warning: {directory} exists as a file")
                directory.parent.mkdir(parents=True, exist_ok=True)
            else:
                directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Warning: Failed to create directory {directory}: {e}")

    # Creating Workday directories
    for directory in [WORKDAY_DIR, PERSON_ACCOUNTS_DIR, PEOPLE_DIR, USED_ENTRIES_DIR]:
        try:
            if directory.exists() and directory.is_file():
                print(f"Warning: {directory} exists as a file")
                directory.parent.mkdir(parents=True, exist_ok=True)
            else:
                directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Warning: Failed to create directory {directory}: {e}")

    # Processing Calabrio directories
    try:
        # Create parent directory for config_data.json file
        if not CONFIG_DATA_PATH.parent.exists():
            CONFIG_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Warning: Failed to create Calabrio directory: {e}")

    # Creating log directory
    try:
        UPLOAD_LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Warning: Failed to create upload log directory: {e}")

# Functions to get data file paths
def get_workday_file(filename):
    """Get full path for Workday files."""
    return WORKDAY_DIR / filename

def get_calabrio_file(filename):
    """Get full path for Calabrio files."""
    return CALABRIO_DIR / filename

def get_config_file(filename):
    """Get full path for configuration files."""
    return CONFIG_DIR / filename  # Correction: Use CONFIG_DIR instead of CONFIG_DATA_PATH

def get_log_file(filename):
    """Get full path for log files."""
    return UPLOAD_LOG_DIR / filename
