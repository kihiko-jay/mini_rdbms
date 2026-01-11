"""
Hash-based index implementation for the mini RDBMS.

Provides O(1) lookups for PRIMARY KEY and UNIQUE columns.
"""

from typing import Any, Dict, List, Optional


class Index:
    """
    Hash index for column values.
    
    Maps column values to row indices for fast lookups.
    
    Note: This is a simplified index that only handles unique values
    (PRIMARY KEY and UNIQUE constraints). For non-unique indexes,
    we would need to map values to lists of row indices.
    """
    
    def __init__(self, column_name: str):
        """
        Initialize an empty index.
        
        Args:
            column_name: Name of the column being indexed
        """
        self.column_name = column_name
        self._index: Dict[Any, int] = {}  # value -> row_index
        self._reverse_index: Dict[int, Any] = {}  # row_index -> value
    
    def add(self, value: Any, row_index: int) -> None:
        """
        Add a value to the index.
        
        Args:
            value: Column value
            row_index: Index of the row in the table
            
        Raises:
            ValueError: If value already exists (for unique index)
        """
        if value in self._index:
            raise ValueError(
                f"Duplicate value '{value}' for indexed column '{self.column_name}'"
            )
        
        self._index[value] = row_index
        self._reverse_index[row_index] = value
    
    def get(self, value: Any) -> Optional[int]:
        """
        Get row index for a value.
        
        Args:
            value: Column value to look up
            
        Returns:
            Row index if found, None otherwise
        """
        return self._index.get(value)
    
    def remove(self, value: Any, row_index: int) -> None:
        """
        Remove a value from the index.
        
        Args:
            value: Column value
            row_index: Expected row index (for validation)
        """
        if value in self._index and self._index[value] == row_index:
            del self._index[value]
        if row_index in self._reverse_index:
            del self._reverse_index[row_index]
    
    def adjust_for_deletion(self, deleted_index: int) -> None:
        """
        Adjust index after a row deletion.
        
        When a row is deleted, all rows with higher indices
        shift down by one. This method updates those indices.
        
        Args:
            deleted_index: Index of the deleted row
        """
        # Update values that point to rows after the deleted one
        new_index: Dict[Any, int] = {}
        new_reverse: Dict[int, Any] = {}
        
        for value, idx in self._index.items():
            if idx > deleted_index:
                new_index[value] = idx - 1
                new_reverse[idx - 1] = value
            elif idx < deleted_index:
                new_index[value] = idx
                new_reverse[idx] = value
            # idx == deleted_index is handled by remove()
        
        self._index = new_index
        self._reverse_index = new_reverse
    
    def update(self, old_value: Any, new_value: Any, row_index: int) -> None:
        """
        Update a value in the index.
        
        Args:
            old_value: Current column value
            new_value: New column value
            row_index: Row index
            
        Raises:
            ValueError: If new value already exists
        """
        self.remove(old_value, row_index)
        self.add(new_value, row_index)
    
    def __contains__(self, value: Any) -> bool:
        """Check if value exists in index."""
        return value in self._index
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"Index({self.column_name}, entries={len(self._index)})"