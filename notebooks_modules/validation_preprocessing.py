import pandas as pd


def preprocess_workday_data(workday_df, people_df):
    """
    Merge Workday data with people data and add MappedEmploymentNumber.
    """
    if not workday_df.empty and not people_df.empty:
        if (
            "WiserId" in workday_df.columns
            and "WiserId" in people_df.columns
            and "Latest Headcount Hire Date" in people_df.columns
            and "Latest Headcount Primary Work Email" in people_df.columns
        ):
            workday_df = pd.merge(
                workday_df,
                people_df[
                    ["WiserId", "Latest Headcount Hire Date", "Latest Headcount Primary Work Email"]
                ],
                on="WiserId",
                how="left",
            )

    # Add MappedEmploymentNumber (using WiserId as fallback)
    workday_df["MappedEmploymentNumber"] = workday_df["WiserId"]
    return workday_df


def preprocess_calabrio_data(calabrio_df, calabrio_person_df):
    """
    Merge person data with calabrio_df.
    """
    if not calabrio_df.empty and not calabrio_person_df.empty:
        if (
            "EmploymentNumber" in calabrio_df.columns
            and "EmploymentNumber" in calabrio_person_df.columns
        ):
            calabrio_df = pd.merge(
                calabrio_df,
                calabrio_person_df[
                    ["EmploymentNumber", "PersonId", "BusinessUnitName", "EmploymentStartDate"]
                ],
                on="EmploymentNumber",
                how="left",
                suffixes=("", "_person"),
            )
    return calabrio_df
