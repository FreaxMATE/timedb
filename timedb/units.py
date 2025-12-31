"""
Unit handling for TimeDB using Pint.

This module provides unit conversion functionality, validating dimensionality
and converting values to canonical series units before storage.

Supports both:
- Pint Quantity objects (e.g., `1.5 * ureg.kW`)
- pint-pandas Series (e.g., `pd.Series([1.5], dtype="pint[kW]")`)
"""
from typing import Optional, Union
import pint
import pandas as pd

# Try to import pint-pandas (optional dependency)
try:
    import pint_pandas
    PINT_PANDAS_AVAILABLE = True
except ImportError:
    PINT_PANDAS_AVAILABLE = False

# Create a Pint UnitRegistry
ureg = pint.UnitRegistry()

# Custom exception for incompatible units
class IncompatibleUnitError(ValueError):
    """Raised when a unit cannot be converted to the target unit due to dimensionality mismatch."""
    pass


def is_pint_pandas_series(series_or_value) -> bool:
    """
    Check if a value is a pint-pandas Series.
    
    Args:
        series_or_value: A pandas Series or any value
    
    Returns:
        True if it's a pint-pandas Series, False otherwise
    """
    if not PINT_PANDAS_AVAILABLE:
        return False
    
    if isinstance(series_or_value, pd.Series):
        # Check if the dtype is a pint dtype (starts with "pint[")
        dtype_str = str(series_or_value.dtype)
        return dtype_str.startswith("pint[") and dtype_str.endswith("]")
    return False


def extract_unit_from_pint_pandas_series(series: pd.Series) -> Optional[str]:
    """
    Extract unit string from a pint-pandas Series.
    
    Args:
        series: A pandas Series with pint dtype (e.g., dtype="pint[MW]" or dtype="pint[megawatt][Float64]")
    
    Returns:
        Unit string (e.g., "MW", "megawatt", "m/s") or None if not a pint-pandas Series
    
    Note:
        Pint-pandas dtype can be in formats like:
        - "pint[MW]"
        - "pint[megawatt][Float64]"
        - "pint[m/s]"
        We extract only the unit part (between the first [ and the first ])
    """
    if not is_pint_pandas_series(series):
        return None
    
    # Extract unit from dtype string
    # Formats can be: "pint[MW]", "pint[megawatt][Float64]", etc.
    dtype_str = str(series.dtype)
    
    # Find the first '[' and the first ']' after it
    # This extracts just the unit part, ignoring any additional brackets
    start_idx = dtype_str.find('[')
    if start_idx == -1:
        return None
    
    end_idx = dtype_str.find(']', start_idx + 1)
    if end_idx == -1:
        return None
    
    # Extract the unit string (between [ and ])
    unit_str = dtype_str[start_idx + 1:end_idx]
    return unit_str


def extract_unit_from_quantity(quantity) -> Optional[str]:
    """
    Extract unit string from a Pint Quantity object or pint-pandas Series.
    
    Args:
        quantity: A Pint Quantity object, pint-pandas Series, or a regular value (float/int)
    
    Returns:
        Unit string (e.g., "kilowatt", "degree_Celsius", "MW", "dimensionless") or None
        Note: For Pint Quantities, returns Pint's internal unit representation.
        For pint-pandas Series, returns the unit as specified in the dtype.
    """
    # Check for pint-pandas Series first
    if isinstance(quantity, pd.Series):
        unit = extract_unit_from_pint_pandas_series(quantity)
        if unit is not None:
            return unit
    
    # Check for Pint Quantity
    if isinstance(quantity, pint.Quantity):
        # Get the unit and convert to string
        # Pint uses its internal representation (e.g., "degree_Celsius" not "degC")
        # This string can be used to recreate the unit later
        return str(quantity.units)
    
    return None


def extract_value_from_quantity(quantity) -> float:
    """
    Extract numeric value from a Pint Quantity object, pint-pandas Series, or regular value.
    
    Args:
        quantity: A Pint Quantity object, pint-pandas Series, or a regular value (float/int)
    
    Returns:
        The numeric value as float
    """
    # For pint-pandas Series, values are already just floats
    if isinstance(quantity, pd.Series):
        if is_pint_pandas_series(quantity):
            # pint-pandas Series values are just regular floats
            return float(quantity.iloc[0] if len(quantity) > 0 else 0.0)
        # Regular pandas Series - return first value
        return float(quantity.iloc[0] if len(quantity) > 0 else 0.0)
    
    # For Pint Quantity objects
    if isinstance(quantity, pint.Quantity):
        return float(quantity.magnitude)
    
    # Regular value
    return float(quantity)


def convert_quantity_to_canonical_unit(
    quantity: pint.Quantity,
    canonical_unit: str,
) -> float:
    """
    Convert a Pint Quantity directly to a canonical unit.
    
    This is the preferred method when you already have a Quantity object,
    as it handles offset units (like Celsius/Fahrenheit) properly.
    
    Args:
        quantity: A Pint Quantity object
        canonical_unit: The target unit for the series (e.g., "MW", "dimensionless", "degree_Celsius")
    
    Returns:
        The converted value as a float (in canonical_unit)
    
    Raises:
        IncompatibleUnitError: If the units have incompatible dimensionality
        ValueError: For other conversion errors
    """
    # Check if units are already the same (no conversion needed)
    if str(quantity.units) == canonical_unit:
        return float(quantity.magnitude)
    
    try:
        canonical_qty = quantity.to(canonical_unit)
        return float(canonical_qty.magnitude)
    except pint.errors.DimensionalityError as e:
        raise IncompatibleUnitError(
            f"Cannot convert {quantity.units} to {canonical_unit}: "
            f"incompatible dimensionality ({e})"
        ) from e
    except pint.errors.OffsetUnitCalculusError as e:
        # For offset units (Celsius, Fahrenheit), Pint requires explicit handling
        # The error typically occurs when the conversion path is ambiguous
        # We'll provide a clearer error message
        raise ValueError(
            f"Cannot convert {quantity.units} to {canonical_unit}: "
            f"ambiguous operation with offset unit. "
            f"This may occur when converting between offset temperature units. "
            f"See https://pint.readthedocs.io/en/stable/user/nonmult.html for guidance. "
            f"Error: {e}"
        ) from e
    except Exception as e:
        raise ValueError(f"Unit conversion error: {e}") from e


def convert_to_canonical_unit(
    value: float,
    submitted_unit: Optional[str],
    canonical_unit: str,
) -> float:
    """
    Convert a value from submitted_unit to canonical_unit using Pint.
    
    Handles offset units (like Celsius/Fahrenheit) properly by using Pint's
    built-in conversion methods.
    
    Args:
        value: The numeric value to convert
        submitted_unit: The unit of the submitted value (e.g., "kW", "MW", "MWh", "degree_Celsius")
                       If None, assumes value is already in canonical_unit
        canonical_unit: The target unit for the series (e.g., "MW", "dimensionless", "degree_Celsius")
    
    Returns:
        The converted value as a float (in canonical_unit)
    
    Raises:
        IncompatibleUnitError: If the units have incompatible dimensionality
        pint.errors.UndefinedUnitError: If a unit string is invalid
        pint.errors.DimensionalityError: If units cannot be converted (caught and re-raised as IncompatibleUnitError)
        ValueError: For other conversion errors including offset unit issues
    
    Examples:
        >>> convert_to_canonical_unit(1000.0, "kW", "MW")
        1.0
        
        >>> convert_to_canonical_unit(100.0, None, "MW")
        100.0
        
        >>> convert_to_canonical_unit(100.0, "kW", "MWh")  # Incompatible
        IncompatibleUnitError: Cannot convert kW to MWh (power vs energy)
    """
    # If no unit provided, assume value is already in canonical unit
    if submitted_unit is None:
        return float(value)
    
    try:
        # Parse the units
        submitted_qty = ureg.Quantity(value, submitted_unit)
        # Use the direct conversion method which handles offset units better
        return convert_quantity_to_canonical_unit(submitted_qty, canonical_unit)
    
    except pint.errors.UndefinedUnitError as e:
        raise ValueError(f"Invalid unit: {e}") from e
    except (IncompatibleUnitError, ValueError):
        # Re-raise these as-is
        raise
    except Exception as e:
        raise ValueError(f"Unit conversion error: {e}") from e


def validate_unit_compatibility(
    submitted_unit: Optional[str],
    canonical_unit: str,
) -> bool:
    """
    Validate that submitted_unit is compatible with canonical_unit.
    
    Args:
        submitted_unit: The unit to validate (None means assume canonical)
        canonical_unit: The target canonical unit
    
    Returns:
        True if compatible, False otherwise
    
    Raises:
        IncompatibleUnitError: If units are incompatible
    """
    if submitted_unit is None:
        return True
    
    try:
        # Try to convert 1.0 from submitted to canonical
        # If this works, units are compatible
        test_qty = ureg.Quantity(1.0, submitted_unit)
        test_qty.to(canonical_unit)
        return True
    
    except pint.errors.DimensionalityError:
        raise IncompatibleUnitError(
            f"Unit {submitted_unit} is incompatible with canonical unit {canonical_unit}"
        )
    
    except Exception:
        # For other errors, assume incompatible
        return False
