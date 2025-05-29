import pandas as pd
import json
from pathlib import Path

def load_workday_data(PERSON_ACCOUNTS_DIR, PEOPLE_DIR, USED_ENTRIES_DIR):
    """Load all Workday data files."""
    workday_df = pd.DataFrame()
    people_df = pd.DataFrame()
    used_entries_df = pd.DataFrame()
    
    # Load person_accounts data
    if PERSON_ACCOUNTS_DIR.exists():
        excel_files = list(PERSON_ACCOUNTS_DIR.glob("*.xlsx"))
        if excel_files:
            latest_file = max(excel_files, key=lambda x: x.stat().st_mtime)
            workday_df = pd.read_excel(latest_file, skiprows=6, engine='openpyxl')
            if 'WiserId' in workday_df.columns:
                workday_df['WiserId'] = workday_df['WiserId'].astype(str)
    
    # Load people data
    if PEOPLE_DIR.exists():
        excel_files = list(PEOPLE_DIR.glob("*.xlsx"))
        if excel_files:
            latest_file = max(excel_files, key=lambda x: x.stat().st_mtime)
            people_df = pd.read_excel(latest_file, skiprows=2, engine='openpyxl')
            if 'Latest Headcount Wiser ID' in people_df.columns:
                people_df.rename(columns={'Latest Headcount Wiser ID': 'WiserId'}, inplace=True)
                people_df['WiserId'] = people_df['WiserId'].astype(str)
            if 'Latest Headcount Hire Date' in people_df.columns:
                people_df['Latest Headcount Hire Date'] = pd.to_datetime(people_df['Latest Headcount Hire Date'])
    
    # Load used entries data
    if USED_ENTRIES_DIR.exists():
        excel_files = list(USED_ENTRIES_DIR.glob("*.xlsx"))
        if excel_files:
            latest_file = max(excel_files, key=lambda x: x.stat().st_mtime)
            used_entries_df = pd.read_excel(latest_file, skiprows=6, engine='openpyxl')
            if 'WiserId' in used_entries_df.columns:
                used_entries_df['WiserId'] = used_entries_df['WiserId'].astype(str)
    
    return workday_df, people_df, used_entries_df

def load_calabrio_data(ACCOUNT_DATA_PATH, PERSON_DATA_PATH):
    """Load all Calabrio data files."""
    calabrio_df = pd.DataFrame()
    person_df = pd.DataFrame()
    
    # Load account data
    if ACCOUNT_DATA_PATH.exists():
        with open(ACCOUNT_DATA_PATH, 'r') as f:
            json_data = json.load(f)
            calabrio_df = pd.DataFrame(json_data)
            if 'EmploymentNumber' in calabrio_df.columns:
                calabrio_df['EmploymentNumber'] = calabrio_df['EmploymentNumber'].astype(str)
            if 'Accrued' in calabrio_df.columns:
                calabrio_df['Accrued'] = pd.to_numeric(calabrio_df['Accrued'], errors='coerce').fillna(0)
    
    # Load person data
    if PERSON_DATA_PATH.exists():
        with open(PERSON_DATA_PATH, 'r') as f:
            json_data = json.load(f)
            person_df = pd.DataFrame(json_data)
            if 'EmploymentNumber' in person_df.columns:
                person_df['EmploymentNumber'] = person_df['EmploymentNumber'].astype(str)
            person_df['EmploymentStartDate'] = pd.to_datetime(person_df['EmploymentStartDate'])
    
    return calabrio_df, person_df
