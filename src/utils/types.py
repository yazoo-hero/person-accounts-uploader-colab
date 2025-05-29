"""Type definitions and enums for the application."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field


class AbsenceType(str, Enum):
    """Enumeration of absence types."""

    ANNUAL_LEAVE = "annual_leave"
    SICK_LEAVE = "sick_leave"
    MATERNITY_LEAVE = "maternity_leave"
    PATERNITY_LEAVE = "paternity_leave"
    UNPAID_LEAVE = "unpaid_leave"
    OTHER = "other"


class DataSource(str, Enum):
    """Data source types."""

    WORKDAY = "workday"
    CALABRIO = "calabrio"


class ValidationStatus(str, Enum):
    """Validation status types."""

    MATCHED = "matched"
    MISMATCHED = "mismatched"
    MISSING = "missing"
    ERROR = "error"


@dataclass
class ColumnMapping:
    """Column mapping configuration."""

    workday_column: str
    calabrio_column: str
    data_type: str
    required: bool = True


class PersonData(BaseModel):
    """Person data model."""

    employment_number: str
    wiser_id: Optional[str] = None
    email: Optional[str] = None
    hire_date: Optional[datetime] = None
    business_unit: Optional[str] = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class AbsenceBalance(BaseModel):
    """Absence balance data model."""

    person_id: str
    absence_type: str
    absence_id: Optional[str] = None
    balance_in: float = Field(default=0.0, ge=0)
    accrued: float = Field(default=0.0, ge=0)
    extra: float = Field(default=0.0, ge=0)
    units_approved: float = Field(default=0.0, ge=0)
    start_date: Optional[datetime] = None
    contract_name: Optional[str] = None
    tracked_by: Optional[str] = None


class ValidationResult(BaseModel):
    """Validation result model."""

    person_id: str
    absence_type: str
    workday_balance: float
    calabrio_balance: float
    expected_balance: float
    balance_match: bool
    accrual_match: bool
    balance_difference: float
    status: ValidationStatus
    error_message: Optional[str] = None


class APIResponse(BaseModel):
    """API response model."""

    success: bool
    data: Optional[Union[Dict, List]] = None
    error: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)
