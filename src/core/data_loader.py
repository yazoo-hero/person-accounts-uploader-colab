"""Data loading functionality with type hints and error handling."""
import pandas as pd
import json
from pathlib import Path
from typing import Tuple, Optional
import logging
from ..utils.exceptions import DataLoadError

logger = logging.getLogger(__name__)


class DataLoader:
    """Handles loading data from various sources."""
    
    @staticmethod
    def load_excel_files(directory: Path, skiprows: int = 0) -> pd.DataFrame:
        """
        Load the latest Excel file from a directory.
        
        Args:
            directory: Path to the directory containing Excel files
            skiprows: Number of rows to skip when reading
            
        Returns:
            DataFrame with loaded data
            
        Raises:
            DataLoadError: If directory doesn't exist or no Excel files found
        """
        if not directory.exists():
            raise DataLoadError(f"Directory does not exist: {directory}")
        
        excel_files = list(directory.glob("*.xlsx"))
        if not excel_files:
            logger.warning(f"No Excel files found in {directory}")
            return pd.DataFrame()
        
        latest_file = max(excel_files, key=lambda x: x.stat().st_mtime)
        logger.info(f"Loading Excel file: {latest_file}")
        
        try:
            return pd.read_excel(latest_file, skiprows=skiprows, engine='openpyxl')
        except Exception as e:
            raise DataLoadError(f"Failed to load Excel file {latest_file}: {e}")
    
    @staticmethod
    def load_json_file(file_path: Path) -> pd.DataFrame:
        """
        Load JSON data into a DataFrame.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            DataFrame with loaded data
            
        Raises:
            DataLoadError: If file doesn't exist or JSON is invalid
        """
        if not file_path.exists():
            raise DataLoadError(f"JSON file does not exist: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            return pd.DataFrame(json_data)
        except json.JSONDecodeError as e:
            raise DataLoadError(f"Invalid JSON in {file_path}: {e}")
        except Exception as e:
            raise DataLoadError(f"Failed to load JSON file {file_path}: {e}")


class WorkdayDataLoader:
    """Loads and processes Workday data."""
    
    def __init__(self, person_accounts_dir: Path, people_dir: Path, used_entries_dir: Path):
        self.person_accounts_dir = person_accounts_dir
        self.people_dir = people_dir
        self.used_entries_dir = used_entries_dir
        self.loader = DataLoader()
    
    def load_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Load all Workday data files.
        
        Returns:
            Tuple of (workday_df, people_df, used_entries_df)
        """
        workday_df = self._load_person_accounts()
        people_df = self._load_people_data()
        used_entries_df = self._load_used_entries()
        
        return workday_df, people_df, used_entries_df
    
    def _load_person_accounts(self) -> pd.DataFrame:
        """Load person accounts data."""
        try:
            df = self.loader.load_excel_files(self.person_accounts_dir, skiprows=6)
            if not df.empty and 'WiserId' in df.columns:
                df['WiserId'] = df['WiserId'].astype(str)
            return df
        except DataLoadError:
            logger.warning("Failed to load person accounts data")
            return pd.DataFrame()
    
    def _load_people_data(self) -> pd.DataFrame:
        """Load people data."""
        try:
            df = self.loader.load_excel_files(self.people_dir, skiprows=2)
            if not df.empty:
                # Rename columns if they exist
                if 'Latest Headcount Wiser ID' in df.columns:
                    df.rename(columns={'Latest Headcount Wiser ID': 'WiserId'}, inplace=True)
                    df['WiserId'] = df['WiserId'].astype(str)
                
                # Convert date columns
                if 'Latest Headcount Hire Date' in df.columns:
                    df['Latest Headcount Hire Date'] = pd.to_datetime(
                        df['Latest Headcount Hire Date'], errors='coerce'
                    )
            return df
        except DataLoadError:
            logger.warning("Failed to load people data")
            return pd.DataFrame()
    
    def _load_used_entries(self) -> pd.DataFrame:
        """Load used entries data."""
        try:
            df = self.loader.load_excel_files(self.used_entries_dir, skiprows=6)
            if not df.empty and 'WiserId' in df.columns:
                df['WiserId'] = df['WiserId'].astype(str)
            return df
        except DataLoadError:
            logger.warning("Failed to load used entries data")
            return pd.DataFrame()


class CalabrioDataLoader:
    """Loads and processes Calabrio data."""
    
    def __init__(self, account_data_path: Path, person_data_path: Path):
        self.account_data_path = account_data_path
        self.person_data_path = person_data_path
        self.loader = DataLoader()
    
    def load_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load all Calabrio data files.
        
        Returns:
            Tuple of (calabrio_df, person_df)
        """
        calabrio_df = self._load_account_data()
        person_df = self._load_person_data()
        
        return calabrio_df, person_df
    
    def _load_account_data(self) -> pd.DataFrame:
        """Load account data."""
        try:
            df = self.loader.load_json_file(self.account_data_path)
            if not df.empty:
                if 'EmploymentNumber' in df.columns:
                    df['EmploymentNumber'] = df['EmploymentNumber'].astype(str)
                if 'Accrued' in df.columns:
                    df['Accrued'] = pd.to_numeric(df['Accrued'], errors='coerce').fillna(0)
            return df
        except DataLoadError:
            logger.warning("Failed to load Calabrio account data")
            return pd.DataFrame()
    
    def _load_person_data(self) -> pd.DataFrame:
        """Load person data."""
        try:
            df = self.loader.load_json_file(self.person_data_path)
            if not df.empty:
                if 'EmploymentNumber' in df.columns:
                    df['EmploymentNumber'] = df['EmploymentNumber'].astype(str)
                if 'EmploymentStartDate' in df.columns:
                    df['EmploymentStartDate'] = pd.to_datetime(
                        df['EmploymentStartDate'], errors='coerce'
                    )
            return df
        except DataLoadError:
            logger.warning("Failed to load Calabrio person data")
            return pd.DataFrame()