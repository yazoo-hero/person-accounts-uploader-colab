"""Mapping utilities for business units and other entities."""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Business unit mapping configuration
BUSINESS_UNIT_MAPPING = {
    # Exact mappings
    "global security": "Global Security",
    "global - security": "Global Security",
    "security": "Global Security",
    "customer care": "Customer Care",
    "global - customer care": "Customer Care",
    "care": "Customer Care",
    "collections": "Collections",
    "global - collections": "Collections",
    "it": "IT",
    "information technology": "IT",
    "global - it": "IT",
    "hr": "Human Resources",
    "human resources": "Human Resources",
    "global - hr": "Human Resources",
    "finance": "Finance",
    "global - finance": "Finance",
    "operations": "Operations",
    "global - operations": "Operations",
    "sales": "Sales",
    "global - sales": "Sales",
    "marketing": "Marketing",
    "global - marketing": "Marketing",
}


def map_business_unit(business_unit: Optional[str]) -> Optional[str]:
    """
    Map business unit names to standardized format.

    Args:
        business_unit: The business unit name to map

    Returns:
        Standardized business unit name or None if not found
    """
    if not business_unit:
        return None

    # First try exact match (case-insensitive)
    bu_lower = business_unit.lower().strip()
    if bu_lower in BUSINESS_UNIT_MAPPING:
        return BUSINESS_UNIT_MAPPING[bu_lower]

    # Try removing common prefixes
    prefixes = ["global -", "global_", "global"]
    for prefix in prefixes:
        if bu_lower.startswith(prefix):
            cleaned = bu_lower[len(prefix) :].strip()
            if cleaned in BUSINESS_UNIT_MAPPING:
                return BUSINESS_UNIT_MAPPING[cleaned]

    # Try partial matching
    for key, value in BUSINESS_UNIT_MAPPING.items():
        if key in bu_lower or bu_lower in key:
            return value

    # Return original if no mapping found
    logger.debug(f"No mapping found for business unit: {business_unit}")
    return business_unit


def map_absence_id(row: Dict[str, Any], config_data: Dict[str, Any]) -> Optional[str]:
    """
    Retrieves AbsenceId from config_data using BusinessUnitName and AbsenceName.

    Args:
        row: Dictionary containing BusinessUnitName and AbsenceName
        config_data: Configuration data dictionary

    Returns:
        AbsenceId if found, None otherwise
    """
    business_unit = row.get("BusinessUnitName")
    absence_name = row.get("AbsenceName")

    if not business_unit or not absence_name:
        return None

    # Check if BusinessUnitName exists in config data
    if business_unit not in config_data:
        # If not found, standardize using mapping function
        mapped_bu = map_business_unit(business_unit)
        if mapped_bu is None or mapped_bu not in config_data:
            logger.debug(f"Business unit not found in config: {business_unit}")
            return None
        business_unit = mapped_bu

    # Get absences list
    absences = config_data.get(business_unit, {}).get("absences", {}).get("Result", [])

    # Attempt exact match
    for absence in absences:
        if absence.get("Name") == absence_name:
            return absence.get("Id")

    # Attempt case-insensitive match
    absence_name_lower = absence_name.lower()
    for absence in absences:
        if absence.get("Name", "").lower() == absence_name_lower:
            return absence.get("Id")

    # Attempt partial match
    for absence in absences:
        if absence_name_lower in absence.get("Name", "").lower():
            return absence.get("Id")

    # Retry by adding 'Global -' prefix
    if not absence_name.lower().startswith("global -"):
        global_absence = f"Global - {absence_name}"
        for absence in absences:
            if absence.get("Name") == global_absence:
                return absence.get("Id")

    logger.debug(f"No absence ID found for: {business_unit} - {absence_name}")
    return None


def safe_get_column(
    df: Any, column_names: list, default_value: Any = None  # Using Any to avoid pandas import
) -> Any:
    """
    Safely get a column from a DataFrame, trying multiple column names.

    Args:
        df: The DataFrame
        column_names: List of column names to try
        default_value: Default value if no column is found

    Returns:
        The column data or default value
    """
    for col in column_names:
        if hasattr(df, "columns") and col in df.columns:
            return df[col]
    return default_value
