"""Configuration settings for validation."""

from pathlib import Path

# ベースディレクトリ
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
CONFIG_DIR = PROJECT_ROOT / 'config'  # 追加: 設定ディレクトリ
LOGS_DIR = PROJECT_ROOT / 'logs'

# Workdayデータのパス
WORKDAY_DIR = DATA_DIR / 'workday'
PERSON_ACCOUNTS_DIR = WORKDAY_DIR / 'person_accounts'
PEOPLE_DIR = WORKDAY_DIR / 'people'
USED_ENTRIES_DIR = WORKDAY_DIR / 'used_entries'

# Calabrioデータのパス
CALABRIO_DIR = DATA_DIR / 'calabrio'
ACCOUNT_DATA_PATH = CALABRIO_DIR / 'account_data.json'
PERSON_DATA_PATH = CALABRIO_DIR / 'person_data.json'

# 設定ファイルのパス
CONFIG_DATA_PATH = CALABRIO_DIR / 'config_data.json'
BALANCE_RULES_PATH = CONFIG_DIR / 'balance_rules.json'  # 追加: バランスルールのパス

# ログファイルの設定
UPLOAD_LOG_DIR = LOGS_DIR / 'uploads'

# 各ディレクトリを作成
def ensure_directories():
    """必要なディレクトリを作成."""
    # ベースディレクトリの作成
    for directory in [DATA_DIR, CONFIG_DIR, LOGS_DIR]:
        try:
            if directory.exists() and directory.is_file():
                print(f"Warning: {directory} exists as a file")
                directory.parent.mkdir(parents=True, exist_ok=True)
            else:
                directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Warning: Failed to create directory {directory}: {e}")

    # Workdayディレクトリの作成
    for directory in [WORKDAY_DIR, PERSON_ACCOUNTS_DIR, PEOPLE_DIR, USED_ENTRIES_DIR]:
        try:
            if directory.exists() and directory.is_file():
                print(f"Warning: {directory} exists as a file")
                directory.parent.mkdir(parents=True, exist_ok=True)
            else:
                directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Warning: Failed to create directory {directory}: {e}")

    # Calabrioディレクトリの処理
    try:
        # config_data.jsonファイルの親ディレクトリを作成
        if not CONFIG_DATA_PATH.parent.exists():
            CONFIG_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Warning: Failed to create Calabrio directory: {e}")

    # ログディレクトリの作成
    try:
        UPLOAD_LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Warning: Failed to create upload log directory: {e}")

# データファイルのパスを取得する関数
def get_workday_file(filename):
    """Workdayファイルの完全なパスを取得."""
    return WORKDAY_DIR / filename

def get_calabrio_file(filename):
    """Calabrioファイルの完全なパスを取得."""
    return CALABRIO_DIR / filename

def get_config_file(filename):
    """設定ファイルの完全なパスを取得."""
    return CONFIG_DIR / filename  # 修正: CONFIG_DATA_PATHの代わりにCONFIG_DIRを使用

def get_log_file(filename):
    """ログファイルの完全なパスを取得."""
    return UPLOAD_LOG_DIR / filename
