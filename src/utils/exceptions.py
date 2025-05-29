"""Custom exceptions for the application."""

from typing import Any, Optional


class PersonAccountsError(Exception):
    """Base exception for all application errors."""

    def __init__(self, message: str, details: Optional[Any] = None):
        super().__init__(message)
        self.message = message
        self.details = details


class DataLoadError(PersonAccountsError):
    """Raised when data loading fails."""

    pass


class ValidationError(PersonAccountsError):
    """Raised when validation fails."""

    pass


class APIError(PersonAccountsError):
    """Raised when API operations fail."""

    def __init__(
        self, message: str, status_code: Optional[int] = None, details: Optional[Any] = None
    ):
        super().__init__(message, details)
        self.status_code = status_code


class ConfigurationError(PersonAccountsError):
    """Raised when configuration is invalid."""

    pass


class MappingError(PersonAccountsError):
    """Raised when data mapping fails."""

    pass


class CalculationError(PersonAccountsError):
    """Raised when balance calculations fail."""

    pass
