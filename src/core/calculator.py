"""Balance calculation logic with configurable rules."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ..utils.exceptions import CalculationError, ConfigurationError

logger = logging.getLogger(__name__)


class BalanceCalculator:
    """Calculates correct balance values based on configurable rules."""

    def __init__(self, rules_file: Path):
        """
        Initialize calculator with rules file.

        Args:
            rules_file: Path to JSON file containing balance rules
        """
        self.rules_file = rules_file
        self.rules = self._load_rules()

    def _load_rules(self) -> Dict[str, Any]:
        """Load balance calculation rules from JSON file."""
        if not self.rules_file.exists():
            logger.warning(f"Rules file not found: {self.rules_file}")
            return self._get_default_rules()

        try:
            with open(self.rules_file, "r", encoding="utf-8") as f:
                rules = json.load(f)
            logger.info(f"Loaded balance rules from {self.rules_file}")
            return rules
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON in rules file: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading rules file: {e}")

    def _get_default_rules(self) -> Dict[str, Any]:
        """Get default balance calculation rules."""
        return {
            "default_values": {"beginning_year_balance": 0, "accrued_this_year": 0},
            "absence_rules": {
                "annual leave": {
                    "calculation_method": "standard",
                    "max_carryover": 40,
                    "accrual_rate": 1.67,
                },
                "sick leave": {
                    "calculation_method": "fixed",
                    "fixed_balance": 5,
                    "accrual_rate": 0,
                },
                "maternity leave": {
                    "calculation_method": "fixed",
                    "fixed_balance": 0,
                    "accrual_rate": 0,
                },
                "paternity leave": {
                    "calculation_method": "fixed",
                    "fixed_balance": 0,
                    "accrual_rate": 0,
                },
            },
            "global_settings": {"round_to_nearest": 1, "min_balance": 0, "max_balance": 999},
        }

    def calculate_correct_values(
        self, row: Dict[str, Any], absence_type: str
    ) -> Tuple[float, float]:
        """
        Calculate correct balance and accrual values for a given row.

        Args:
            row: Dictionary containing employee data
            absence_type: Type of absence (e.g., "Annual Leave")

        Returns:
            Tuple of (correct_balance_in, correct_accrued)
        """
        try:
            # Get absence-specific rules
            absence_key = absence_type.lower() if absence_type else ""
            absence_rules = self.rules.get("absence_rules", {}).get(
                absence_key, self.rules.get("absence_rules", {}).get("default", {})
            )

            # Get calculation method
            calc_method = absence_rules.get("calculation_method", "standard")

            if calc_method == "fixed":
                return self._calculate_fixed_values(row, absence_rules)
            elif calc_method == "standard":
                return self._calculate_standard_values(row, absence_rules)
            elif calc_method == "custom":
                return self._calculate_custom_values(row, absence_rules)
            else:
                logger.warning(f"Unknown calculation method: {calc_method}")
                return self._calculate_standard_values(row, absence_rules)

        except Exception as e:
            logger.error(f"Error calculating values for {absence_type}: {e}")
            raise CalculationError(f"Failed to calculate balance: {e}")

    def _calculate_fixed_values(
        self, row: Dict[str, Any], rules: Dict[str, Any]
    ) -> Tuple[float, float]:
        """Calculate fixed balance values."""
        fixed_balance = float(rules.get("fixed_balance", 0))
        fixed_accrual = float(rules.get("fixed_accrual", 0))

        return fixed_balance, fixed_accrual

    def _calculate_standard_values(
        self, row: Dict[str, Any], rules: Dict[str, Any]
    ) -> Tuple[float, float]:
        """Calculate standard balance values based on employment data."""
        # Get default values
        defaults = self.rules.get("default_values", {})
        beginning_balance = float(
            row.get("Beginning Year Balance", defaults.get("beginning_year_balance", 0))
        )
        accrued_this_year = float(
            row.get("Accrued this year", defaults.get("accrued_this_year", 0))
        )

        # Apply carryover limits
        max_carryover = float(rules.get("max_carryover", float("inf")))
        if beginning_balance > max_carryover:
            beginning_balance = max_carryover

        # Calculate accrual based on employment duration
        hire_date = row.get("Latest Headcount Hire Date") or row.get("EmploymentStartDate")
        if hire_date and rules.get("prorate_first_year", False):
            accrued_this_year = self._prorate_accrual(
                hire_date, accrued_this_year, float(rules.get("accrual_rate", 0))
            )

        # Apply global settings
        global_settings = self.rules.get("global_settings", {})
        correct_balance = self._apply_limits(
            beginning_balance,
            global_settings.get("min_balance", 0),
            global_settings.get("max_balance", 999),
        )
        correct_accrual = self._apply_limits(
            accrued_this_year, 0, global_settings.get("max_accrual", 999)
        )

        # Round values
        round_to = global_settings.get("round_to_nearest", 1)
        correct_balance = round(correct_balance / round_to) * round_to
        correct_accrual = round(correct_accrual / round_to) * round_to

        return correct_balance, correct_accrual

    def _calculate_custom_values(
        self, row: Dict[str, Any], rules: Dict[str, Any]
    ) -> Tuple[float, float]:
        """Calculate custom balance values using formula."""
        # This is a placeholder for custom calculation logic
        # Could be extended to support formula evaluation
        formula = rules.get("formula", "")
        if not formula:
            return self._calculate_standard_values(row, rules)

        # For now, just use standard calculation
        logger.warning(f"Custom formula not implemented: {formula}")
        return self._calculate_standard_values(row, rules)

    def _prorate_accrual(self, hire_date: Any, full_accrual: float, monthly_rate: float) -> float:
        """Prorate accrual based on hire date."""
        if not hire_date:
            return full_accrual

        try:
            # Convert hire_date to datetime if needed
            if isinstance(hire_date, str):
                hire_date = datetime.strptime(hire_date, "%Y-%m-%d")
            elif hasattr(hire_date, "to_pydatetime"):
                hire_date = hire_date.to_pydatetime()

            # Calculate months employed this year
            current_year = datetime.now().year
            if hire_date.year < current_year:
                return full_accrual

            months_employed = 12 - hire_date.month + 1
            prorated = months_employed * monthly_rate

            return min(prorated, full_accrual)

        except Exception as e:
            logger.warning(f"Error prorating accrual: {e}")
            return full_accrual

    def _apply_limits(self, value: float, min_val: float, max_val: float) -> float:
        """Apply minimum and maximum limits to a value."""
        return max(min_val, min(value, max_val))
