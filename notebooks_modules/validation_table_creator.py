import pandas as pd
import json
from pathlib import Path
from notebooks_modules.validation_utils import map_business_unit, safe_get_column
from notebooks_modules.validation_calculator import BalanceCalculator

def map_absence_id(row, config_data):
    """
    Retrieves AbsenceId from config_data using BusinessUnitName and AbsenceName.
    
    Args:
        row: DataFrame row
        config_data: Configuration data dictionary
    
    Returns:
        str: AbsenceId, or None if not found
    """
    business_unit = row['BusinessUnitName']
    absence_name = row['AbsenceName']
    
    # Check if BusinessUnitName exists in config data
    if business_unit not in config_data:
        # If not found, standardize using mapping function
        mapped_bu = map_business_unit(business_unit)
        if mapped_bu is None or mapped_bu not in config_data:
            return None
        business_unit = mapped_bu
    
    # Get absences list
    absences = config_data.get(business_unit, {}).get('absences', {}).get('Result', [])
    
    # Attempt exact match
    for absence in absences:
        if absence.get('Name') == absence_name:
            return absence.get('Id')
    
    # Attempt case-insensitive match
    absence_name_lower = absence_name.lower()
    for absence in absences:
        if absence.get('Name', '').lower() == absence_name_lower:
            return absence.get('Id')
    
    # Attempt partial match
    for absence in absences:
        if absence_name_lower in absence.get('Name', '').lower():
            return absence.get('Id')
    
    # Retry by adding 'Global -' prefix
    if not absence_name.lower().startswith('global -'):
        global_absence = f"Global - {absence_name}"
        for absence in absences:
            if absence.get('Name') == global_absence:
                return absence.get('Id')
    
    return None

def add_absence_id_to_df(df, config_data):
    """
    Adds an AbsenceId column to the DataFrame.
    
    Args:
        df: Input DataFrame
        config_data: Configuration data dictionary
    
    Returns:
        DataFrame: DataFrame with AbsenceId column added
    """
    # List to store results
    absence_ids = []
    
    # Process each row
    for _, row in df.iterrows():
        absence_id = map_absence_id(row, config_data)
        absence_ids.append(absence_id)
    
    # Create result DataFrame
    result_df = df.copy()
    result_df['AbsenceId'] = absence_ids
    
    # Mapping results statistics
    mapped_count = sum(1 for aid in absence_ids if aid is not None)
    total_count = len(absence_ids)
    print(f"Mapping results: {mapped_count}/{total_count} ({mapped_count/total_count*100:.1f}%) records mapped with AbsenceId")

    return result_df

def create_validation_table(workday_df, calabrio_df, CONFIG_DIR):
    """Create validation table comparing Workday and Calabrio data."""
    # Clean and convert columns
    workday_df = workday_df.copy()
    calabrio_df = calabrio_df.copy()
    
    workday_df["WiserId"] = workday_df["WiserId"].fillna("").astype(str).str.strip().str.lower()
    workday_df["Original_AbsenceType_Case"] = workday_df["AbsenceType"].fillna("").astype(str).str.strip()
    workday_df["AbsenceType"] = workday_df["AbsenceType"].fillna("").astype(str).str.strip().str.lower()
    
    calabrio_df["EmploymentNumber"] = calabrio_df["EmploymentNumber"].fillna("").astype(str).str.strip().str.lower()
    calabrio_df["AbsenceName"] = calabrio_df["AbsenceName"].fillna("").astype(str).str.strip().str.lower()
    
    # Group Calabrio data
    calabrio_grouped = calabrio_df.groupby(["EmploymentNumber", "AbsenceName"]).first().reset_index()
    
    # Merge data
    merged_df = pd.merge(
        workday_df,
        calabrio_grouped,
        left_on=["MappedEmploymentNumber", "AbsenceType"],
        right_on=["EmploymentNumber", "AbsenceName"],
        how="left"
    )
    
    # Create display DataFrame
    display_df = pd.DataFrame({
        "Workday Person Number": merged_df["WiserId"].fillna(merged_df["EmploymentNumber"]),
        "Calabrio Person Number": merged_df["EmploymentNumber"],
        "Workday Absence Type": merged_df["Original_AbsenceType_Case"],
        "Calabrio Absence Type": merged_df["AbsenceName"],
        "Absence ID": merged_df["AbsenceId"],
        "Calabrio BusinessUnitName": merged_df["BusinessUnitName"],
        "StartDate": merged_df["StartDate"],
        "ContractName": merged_df["ContractName"],
        "Calabrio Balance In": pd.to_numeric(merged_df["BalanceIn"], errors="coerce").fillna(0).round().astype("Int64"),
        "Calabrio_Accrued": pd.to_numeric(merged_df["Accrued"].fillna(0), errors="coerce").fillna(0).round().astype("Int64"),
        "Calabrio Extra": pd.to_numeric(merged_df["Extra"].fillna(0), errors="coerce").fillna(0).round().astype("Int64"),
        "Units Approved": pd.to_numeric(safe_get_column(merged_df, ["Units Approved"], 0), errors="coerce").fillna(0).round().astype("Int64"),
        "TrackedBy": merged_df["TrackedBy"],
        "Calabrio PersonId": merged_df["PersonId"],
        "Beginning Year Balance": pd.to_numeric(safe_get_column(merged_df, ["Beginning Year Balance"], 0), errors="coerce").fillna(0),
        "Accrued this year": pd.to_numeric(safe_get_column(merged_df, ["Accrued this year"], 0), errors="coerce").fillna(0),
        "Latest Headcount Primary Work Email": merged_df["Latest Headcount Primary Work Email"],
        "EmploymentStartDate": merged_df["EmploymentStartDate"],
        "Latest Headcount Hire Date": merged_df["Latest Headcount Hire Date"]
    })
    
    # Calculate correct values
    calculator = BalanceCalculator(Path(CONFIG_DIR, 'balance_rules.json'))
    display_df["Correct Balance In"], display_df["Correct_Accrued"] = zip(
        *display_df.apply(lambda row: calculator.calculate_correct_values(row, row["Workday Absence Type"]), axis=1)
    )
    
    # Calculate matches
    display_df["Balance Match"] = display_df.apply(
        lambda row: "✅" if row["Correct Balance In"] == row["Calabrio Balance In"] else "❌", axis=1
    )
    display_df["Accrual Match"] = display_df.apply(
        lambda row: "✅" if row["Correct_Accrued"] == row["Calabrio_Accrued"] else "❌", axis=1
    )
    
    # Calculate balance difference
    display_df["Balance Difference"] = display_df["Correct Balance In"] - display_df["Calabrio Balance In"]
    
    return display_df
