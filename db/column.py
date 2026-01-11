"""
Column definition for the mini RDBMS.

Represents a single column with name, data type, and constraints.
"""

from typing import Any, Optional
from enum import Enum


class DataType(Enum):
    """Supported data types."""
    INT = "INT"
    TEXT = "TEXT"


class Column:
    """
    A database column definition.
    
    Attributes:
        name: Column name
        data_type: INT or TEXT
        is_primary_key: Whether column is PRIMARY KEY
        is_unique: Whether column has UNIQUE constraint
    """
    
    def __init__(self, name: str, data_type: DataType, 
                 is_primary_key: bool = False, is_unique: bool = False):
        """
        Initialize a column definition.
        
        Args:
            name: Column name
            data_type: DataType.INT or DataType.TEXT
            is_primary_key: Whether this is a PRIMARY KEY column
            is_unique: Whether this column has UNIQUE constraint
            
        Raises:
            ValueError: If constraints conflict (PRIMARY KEY implies UNIQUE)
        """
        self.name = name
        self.data_type = data_type
        self.is_primary_key = is_primary_key
        self.is_unique = is_unique or is_primary_key  # PK implies unique
        
        # Validate constraint combination
        if is_primary_key and not is_unique:
            # This shouldn't happen with the above logic, but just in case
            self.is_unique = True
    
    def validate_value(self, value: Any) -> Any:
        """
        Validate and convert a value for this column.
        
        Args:
            value: The value to validate
            
        Returns:
            The validated value (converted to proper type)
            
        Raises:
            TypeError: If value type doesn't match column type
            ValueError: If value is invalid for type
        """
        if value is None:
            # Allow NULL values for now (simplified)
            return None
        
        try:
            if self.data_type == DataType.INT:
                # Try to convert to int
                return int(value)
            elif self.data_type == DataType.TEXT:
                # Convert to string
                return str(value)
            else:
                # Should not happen if we only have two types
                raise TypeError(f"Unknown data type: {self.data_type}")
        except (ValueError, TypeError) as e:
            raise TypeError(
                f"Value '{value}' cannot be converted to {self.data_type.value}"
            ) from e
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        constraints = []
        if self.is_primary_key:
            constraints.append("PRIMARY KEY")
        elif self.is_unique:
            constraints.append("UNIQUE")
        
        constraint_str = " ".join(constraints)
        if constraint_str:
            constraint_str = " " + constraint_str
        
        return f"Column({self.name} {self.data_type.value}{constraint_str})"