import json
from datetime import datetime, date
import math
import pandas as pd

class BalanceCalculator:
    """Calculates correct balance and accrual based on rules."""

    def __init__(self, rules_path):
        self.rules = self._load_rules(rules_path)

    def _load_rules(self, rules_path):
        """Load rules from a JSON file."""
        try:
            with open(rules_path, 'r') as f:
                rules = json.load(f)['rules']
                # Sort rules by priority
                return sorted(rules, key=lambda x: x.get('priority', 999))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading rules from {rules_path}: {e}")
            return []
        except KeyError as e:
            print(f"Error: 'rules' key not found in {rules_path}: {e}")
            return []

    def _get_year_start_date(self, row):
        """
        Calculate YearStartDate for accrual calculation.
        
        Uses Latest Headcount Hire Date to determine the start date:
        - If hired before 2025: use January 1st, 2025
        - If hired in 2025: use actual hire date
        """
        current_year = datetime.now().year
        default_start = date(current_year, 1, 1)

        try:
            # Try to get Latest Headcount Hire Date first
            hire_date_str = row.get('Latest Headcount Hire Date')
            if not hire_date_str or pd.isna(hire_date_str) or hire_date_str in ('', 'N/A'):
                # If Latest Headcount Hire Date is not available, try EmploymentStartDate
                hire_date_str = row.get('EmploymentStartDate')
                if not hire_date_str or pd.isna(hire_date_str) or hire_date_str in ('', 'N/A'):
                    return default_start
            
            # Convert to datetime, handling various formats
            try:
                # Try to parse the date string
                if isinstance(hire_date_str, str):
                    # Handle ISO format with T separator
                    if 'T' in hire_date_str:
                        hire_date_str = hire_date_str.split('T')[0]
                    
                hire_date = pd.to_datetime(hire_date_str, errors="coerce")
                if pd.isna(hire_date):
                    return default_start
            except Exception:
                return default_start

            # If hire date is before current year, use January 1st
            if hire_date.year < current_year:
                return default_start
            
            # If hire date is in current year, use actual hire date
            if hire_date.year == current_year:
                return hire_date.date()
            
            # If hire date is future year (shouldn't happen), use default
            return default_start

        except Exception as e:
            print(f"Error in _get_year_start_date: {str(e)}")
            return default_start

    def _calculate_quarter(self, target_date):
        """Calculate quarter from a date."""
        if not target_date:
            return None
        return math.ceil(target_date.month / 3)

    def _calculate_remaining_quarters(self, year_start_date):
        """Calculate number of quarters remaining after the start date's quarter."""
        if not year_start_date:
            return 0
        
        # Identify the current quarter
        start_quarter = self._calculate_quarter(year_start_date)
        
        # Calculate the number of remaining quarters until the end of the year
        return max(4 - start_quarter, 0)

    def _calculate_proration_ratio(self, year_start_date):
        """Calculate proration ratio based on YearStartDate."""
        if not year_start_date:
            return 0

        current_year = datetime.now().year
        year_end = date(current_year, 12, 31)
        total_days = (date(current_year, 12, 31) - date(current_year, 1, 1)).days + 1
        remaining_days = (year_end - year_start_date).days + 1

        return remaining_days / total_days

    def _calculate_week_proration_ratio(self, year_start_date):
        """Calculate week-based proration ratio based on YearStartDate."""
        if not year_start_date:
            return 0
        try:
            join_week = year_start_date.isocalendar()[1]
        except Exception:
            return 0
        remaining_weeks = max(52 - join_week + 1, 0)
        return remaining_weeks / 52

    def _calculate_me_day_balance(self, year_start_date, is_usa=False):
        """Calculate Me Day balance based on remaining quarters."""
        if not year_start_date:
            return 0

        remaining_quarters = self._calculate_remaining_quarters(year_start_date)
        
        if is_usa:
            # USA: 480 minutes (8 hours) per quarter
            return remaining_quarters * 480
        else:
            # Global: 1 day per quarter
            return remaining_quarters

    def _conditions_met(self, row, conditions):
        """Check if all conditions in a rule are met."""
        for condition_key, condition_value in conditions.items():
            row_value = str(row.get(condition_key, '')).strip().upper()
            condition_value = str(condition_value).strip().upper()
            
            if row_value != condition_value:
                return False
        return True

    def _calculate_accrual(self, rule, year_start_date):
        """Calculate accrual based on rule type."""
        accrual_type = rule.get('accrual_calculation')
        default_accrual = rule.get('default_accrual', 0)
        absence_type = rule.get('conditions', {}).get('AbsenceType', '')

        if accrual_type == 'fixed':
            return default_accrual

        elif accrual_type == 'prorate':
            if default_accrual in (12000, 7200):  # USA - PTO and USA - Sickness
                ratio = self._calculate_week_proration_ratio(year_start_date)
                return math.floor(default_accrual * ratio)
            else:
                ratio = self._calculate_proration_ratio(year_start_date)
                return math.floor(default_accrual * ratio)

        elif accrual_type == 'me_day_global':
            return self._calculate_me_day_balance(year_start_date, is_usa=False)

        elif accrual_type == 'me_day_usa':
            return self._calculate_me_day_balance(year_start_date, is_usa=True)

        else:
            return 0

    def calculate_correct_values(self, row, original_absence_type=None):
        """Calculate correct balance and accrual based on the rules."""
        try:
            balance = float(row.get('Beginning Year Balance', 0))
        except (ValueError, TypeError):
            balance = 0
            
        year_start_date = self._get_year_start_date(row)
        
        absence_type = original_absence_type if original_absence_type is not None else row.get('AbsenceType', 'Unknown')
        normalized_absence = str(absence_type).lower().strip()
        
        # Convert Workday balance (hours) to minutes for USA Absence Types
        if normalized_absence.startswith('usa -'):
            balance = balance * 60
        
        # Find the first matching rule for accrual calculation
        for rule in self.rules:
            # Use a temporary dict with the original absence type if provided
            if original_absence_type is not None:
                temp_row = {'AbsenceType': original_absence_type}
            else:
                temp_row = row
            
            rule_conditions = rule['conditions']
            
            if self._conditions_met(temp_row, rule_conditions):
                accrual = self._calculate_accrual(rule, year_start_date)
                
                # Special cases
                if normalized_absence == "est - vacation plan (days)":
                    balance = max(0, balance)  # Clip negative balance to 0
                
                # Round the balance after all calculations
                balance = round(balance)
                
                return balance, accrual

        # If no rule matches, return Workday values
        accrual = row.get('Accrued this year', 0)
        accrual = round(accrual)
        balance = round(balance)

        return balance, accrual
