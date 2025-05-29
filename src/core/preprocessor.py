"""Data preprocessing functionality with type hints."""

import logging

import pandas as pd

from ..utils.exceptions import ValidationError

logger = logging.getLogger(__name__)


class DataPreprocessor:
    """Handles data preprocessing and merging operations."""

    @staticmethod
    def merge_workday_with_people(
        workday_df: pd.DataFrame, people_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge Workday data with people data and add MappedEmploymentNumber.

        Args:
            workday_df: Workday DataFrame
            people_df: People DataFrame

        Returns:
            Merged DataFrame with MappedEmploymentNumber
        """
        if workday_df.empty:
            logger.warning("Workday DataFrame is empty")
            return workday_df

        if people_df.empty:
            logger.warning("People DataFrame is empty")
            workday_df["MappedEmploymentNumber"] = workday_df.get("WiserId", pd.Series())
            return workday_df

        # Check required columns
        required_workday_cols = ["WiserId"]
        required_people_cols = [
            "WiserId",
            "Latest Headcount Hire Date",
            "Latest Headcount Primary Work Email",
        ]

        if not all(col in workday_df.columns for col in required_workday_cols):
            raise ValidationError(f"Workday data missing required columns: {required_workday_cols}")

        if not all(col in people_df.columns for col in required_people_cols):
            logger.warning(f"People data missing some columns from: {required_people_cols}")
            # Proceed with available columns
            merge_cols = [col for col in required_people_cols if col in people_df.columns]
            if "WiserId" not in merge_cols:
                workday_df["MappedEmploymentNumber"] = workday_df["WiserId"]
                return workday_df
        else:
            merge_cols = required_people_cols

        # Perform merge
        result_df = pd.merge(workday_df, people_df[merge_cols], on="WiserId", how="left")

        # Add MappedEmploymentNumber (using WiserId as fallback)
        result_df["MappedEmploymentNumber"] = result_df["WiserId"]

        logger.info(
            f"Merged {len(workday_df)} Workday records with {len(people_df)} people records"
        )

        return result_df

    @staticmethod
    def merge_calabrio_with_person(
        calabrio_df: pd.DataFrame, person_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge Calabrio account data with person data.

        Args:
            calabrio_df: Calabrio accounts DataFrame
            person_df: Calabrio person DataFrame

        Returns:
            Merged DataFrame
        """
        if calabrio_df.empty:
            logger.warning("Calabrio DataFrame is empty")
            return calabrio_df

        if person_df.empty:
            logger.warning("Person DataFrame is empty")
            return calabrio_df

        # Check required columns
        if "EmploymentNumber" not in calabrio_df.columns:
            raise ValidationError("Calabrio data missing 'EmploymentNumber' column")

        if "EmploymentNumber" not in person_df.columns:
            raise ValidationError("Person data missing 'EmploymentNumber' column")

        # Select columns to merge
        merge_cols = ["EmploymentNumber"]
        optional_cols = ["PersonId", "BusinessUnitName", "EmploymentStartDate"]

        for col in optional_cols:
            if col in person_df.columns:
                merge_cols.append(col)

        # Perform merge
        result_df = pd.merge(
            calabrio_df,
            person_df[merge_cols],
            on="EmploymentNumber",
            how="left",
            suffixes=("", "_person"),
        )

        logger.info(
            f"Merged {len(calabrio_df)} Calabrio records with {len(person_df)} person records"
        )

        return result_df

    @staticmethod
    def standardize_column_types(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column data types for consistency.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with standardized types
        """
        df = df.copy()

        # String columns that should be lowercase for matching
        lowercase_cols = ["WiserId", "EmploymentNumber", "AbsenceType", "AbsenceName"]
        for col in lowercase_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip().str.lower()

        # Numeric columns
        numeric_cols = [
            "BalanceIn",
            "Accrued",
            "Extra",
            "Units Approved",
            "Beginning Year Balance",
            "Accrued this year",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Date columns
        date_cols = ["StartDate", "EmploymentStartDate", "Latest Headcount Hire Date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        return df
