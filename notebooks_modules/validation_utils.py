"""Validation utility functions."""

import pandas as pd
import numpy as np
from validation_config import (
    WORKDAY_DIR, CALABRIO_DIR, CONFIG_DATA_PATH,
    get_workday_file, get_calabrio_file, get_config_file
)

def convert_to_upload_format(row):
    """Convert a row to upload TSV format."""
    person_id = row.get('Calabrio PersonId', '')
    absence_id = row.get('Absence ID')
    balance = row.get('Correct Balance In', row.get('Calabrio Balance In', 0))
    accrued = row.get('Correct_Accrued', 0)
    extra = row.get('Calabrio Extra', 0)
    
    # Convert to integers
    balance_int = int(balance) if pd.notna(balance) and str(balance).strip() != "" else 0
    accrued_int = int(accrued) if pd.notna(accrued) and str(accrued).strip() != "" else 0
    extra_int = int(extra) if pd.notna(extra) and str(extra).strip() != "" else 0
    
    return f"{person_id}\t{absence_id}\t{balance_int}\t{accrued_int}\t{extra_int}"

def create_filter_options(validation_df):
    """Create filter options from validation DataFrame."""
    absence_types = [{'label': str(val), 'value': str(val)} 
                    for val in validation_df['Workday Absence Type'].dropna().unique()]
    contracts = [{'label': str(val), 'value': str(val)} 
                for val in validation_df['ContractName'].dropna().unique()]
    balance_matches = [{'label': '一致', 'value': '✅'}, {'label': '不一致', 'value': '❌'}]
    accrual_matches = [{'label': '一致', 'value': '✅'}, {'label': '不一致', 'value': '❌'}]
    
    return {
        'absence_types': absence_types,
        'contracts': contracts,
        'balance_matches': balance_matches,
        'accrual_matches': accrual_matches
    }

def filter_validation_data(df, absence_types=None, contracts=None, balance_matches=None, accrual_matches=None):
    """Filter validation DataFrame based on criteria."""
    filtered_df = df.copy()
    
    if absence_types:
        filtered_df = filtered_df[filtered_df['Workday Absence Type'].isin(absence_types)]
    
    if contracts:
        filtered_df = filtered_df[filtered_df['ContractName'].isin(contracts)]
    
    if balance_matches:
        filtered_df = filtered_df[filtered_df['Balance Match'].isin(balance_matches)]
    
    if accrual_matches:
        filtered_df = filtered_df[filtered_df['Accrual Match'].isin(accrual_matches)]
    
    return filtered_df

def safe_get_column(df, possible_names, default_value=None):
    """Safely get a column from a DataFrame using a list of possible column names."""
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")

    for name in possible_names:
        if name in df.columns:
            series = df[name]
            # First try to convert string representations of numbers
            try:
                # Handle numeric columns
                clean_series = series.replace([None, 'nan', 'NaN', ''], np.nan)
                return pd.to_numeric(clean_series, errors='coerce').fillna(default_value if default_value is not None else 0)
            except:
                pass

            # If not numeric, try datetime
            try:
                # Handle potential datetime columns
                return pd.to_datetime(series, errors='coerce').fillna(default_value if default_value is not None else pd.NaT)
            except:
                # Handle string columns
                return series.fillna(default_value if default_value is not None else '')
    
    # If column not found, return default series
    return pd.Series([default_value] * len(df))
