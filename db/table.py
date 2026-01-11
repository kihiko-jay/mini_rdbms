"""
Table implementation for the mini RDBMS.

Manages rows, enforces constraints, and provides CRUD operations.
"""

from typing import Any, Dict, List, Optional, Tuple
from .column import Column, DataType
from .index import Index


class Table:
    """
    A database table with rows and columns.
    
    Attributes:
        name: Table name
        columns: List of Column objects
        rows: List of dictionaries representing rows
        indexes: Dictionary mapping column names to Index objects
    """
    
    def __init__(self, name: str, columns: List[Column]):
        """
        Initialize a table.
        
        Args:
            name: Table name
            columns: List of Column definitions
            
        Raises:
            ValueError: If no columns or multiple PRIMARY KEYs
        """
        if not columns:
            raise ValueError("Table must have at least one column")
        
        # Validate only one primary key
        primary_keys = [col for col in columns if col.is_primary_key]
        if len(primary_keys) > 1:
            raise ValueError("Table can have only one PRIMARY KEY")
        
        self.name = name
        self.columns = columns
        self.rows: List[Dict[str, Any]] = []
        
        # Create indexes for PRIMARY KEY and UNIQUE columns
        self.indexes: Dict[str, Index] = {}
        for column in columns:
            if column.is_primary_key or column.is_unique:
                self.indexes[column.name] = Index(column.name)
        
        # Build column name lookup for faster access
        self.column_dict = {col.name: col for col in columns}
    
    def insert(self, values: List[Any]) -> None:
        """
        Insert a new row into the table.
        
        Args:
            values: List of values in column order
            
        Raises:
            ValueError: If wrong number of values or constraint violation
        """
        if len(values) != len(self.columns):
            raise ValueError(
                f"Expected {len(self.columns)} values, got {len(values)}"
            )
        
        # Create row dictionary with validated values
        row: Dict[str, Any] = {}
        for col, value in zip(self.columns, values):
            validated_value = col.validate_value(value)
            row[col.name] = validated_value
            
            # Check unique constraints before inserting
            if col.is_primary_key or col.is_unique:
                index = self.indexes[col.name]
                if validated_value in index:
                    raise ValueError(
                        f"Duplicate value '{validated_value}' for "
                        f"{'PRIMARY KEY' if col.is_primary_key else 'UNIQUE'} "
                        f"column '{col.name}'"
                    )
        
        # Add row
        self.rows.append(row)
        
        # Update indexes
        for col_name, index in self.indexes.items():
            index.add(row[col_name], len(self.rows) - 1)
    
    def select(self, conditions: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Select rows matching conditions.
        
        Args:
            conditions: Dictionary of column=value conditions (ANDed)
            
        Returns:
            List of matching rows
        """
        if not conditions:
            # Return all rows if no conditions
            return self.rows.copy()
        
        # Use index if available for single equality condition
        if len(conditions) == 1:
            col_name, value = next(iter(conditions.items()))
            if col_name in self.indexes:
                idx = self.indexes[col_name].get(value)
                if idx is not None:
                    return [self.rows[idx].copy()]
                return []
        
        # Otherwise, filter rows
        results = []
        for row in self.rows:
            match = True
            for col_name, value in conditions.items():
                if row.get(col_name) != value:
                    match = False
                    break
            if match:
                results.append(row.copy())
        
        return results
    
    def update(self, updates: Dict[str, Any], conditions: Optional[Dict[str, Any]] = None) -> int:
        """
        Update rows matching conditions.
        
        Args:
            updates: Dictionary of column=new_value
            conditions: Dictionary of column=value conditions (ANDed)
            
        Returns:
            Number of rows updated
            
        Raises:
            ValueError: If constraint violation
        """
        # Find rows to update
        rows_to_update = self.select(conditions) if conditions else self.rows
        
        # Get row indices
        indices = []
        for row in rows_to_update:
            # Find index of this row (simplified - O(n) lookup)
            for i, r in enumerate(self.rows):
                if all(r[col] == row[col] for col in r.keys()):
                    indices.append(i)
                    break
        
        # Validate updates before applying
        for idx in indices:
            row = self.rows[idx]
            # Check unique constraint violations
            for col_name, new_value in updates.items():
                if col_name in self.indexes:
                    col = self.column_dict[col_name]
                    validated_value = col.validate_value(new_value)
                    
                    # Check if new value already exists (excluding current row)
                    index = self.indexes[col_name]
                    existing_idx = index.get(validated_value)
                    if existing_idx is not None and existing_idx != idx:
                        raise ValueError(
                            f"Duplicate value '{validated_value}' for "
                            f"{'PRIMARY KEY' if col.is_primary_key else 'UNIQUE'} "
                            f"column '{col_name}'"
                        )
        
        # Apply updates
        count = 0
        for idx in indices:
            row = self.rows[idx]
            
            # Remove old values from indexes
            for col_name in updates.keys():
                if col_name in self.indexes:
                    old_value = row[col_name]
                    self.indexes[col_name].remove(old_value, idx)
            
            # Update row
            for col_name, new_value in updates.items():
                col = self.column_dict[col_name]
                row[col_name] = col.validate_value(new_value)
                
                # Add new values to indexes
                if col_name in self.indexes:
                    self.indexes[col_name].add(row[col_name], idx)
            
            count += 1
        
        return count
    
    def delete(self, conditions: Optional[Dict[str, Any]] = None) -> int:
        """
        Delete rows matching conditions.
        
        Args:
            conditions: Dictionary of column=value conditions (ANDed)
            
        Returns:
            Number of rows deleted
        """
        # Find rows to delete
        rows_to_delete = self.select(conditions) if conditions else self.rows
        
        # Get row indices in reverse order (for safe deletion)
        indices = []
        for row in rows_to_delete:
            for i, r in enumerate(self.rows):
                if all(r[col] == row[col] for col in r.keys()):
                    indices.append(i)
                    break
        
        # Sort indices in reverse to delete from end
        indices.sort(reverse=True)
        
        # Delete rows
        for idx in indices:
            row = self.rows[idx]
            
            # Remove from indexes
            for col_name, index in self.indexes.items():
                index.remove(row[col_name], idx)
            
            # Adjust index pointers for rows after deleted one
            for col_name, index in self.indexes.items():
                index.adjust_for_deletion(idx)
            
            # Remove row
            del self.rows[idx]
        
        return len(indices)
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        col_defs = ", ".join(str(col) for col in self.columns)
        return f"Table({self.name}, columns=[{col_defs}], rows={len(self.rows)})"