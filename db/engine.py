"""
Database engine - the central coordinator for the mini RDBMS.

Manages tables and coordinates operations.
"""

from typing import Any, Dict, List, Optional
from .table import Table  # Import Table from table.py
from .column import Column, DataType  # Import Column and DataType from column.py


class DatabaseEngine:
    """
    Main database engine that manages tables and operations.
    
    Acts as a simple catalog and router for database operations.
    """
    
    def __init__(self):
        """Initialize an empty database."""
        self.tables: Dict[str, Table] = {}
    
    def create_table(self, table_name: str, columns: List[Column]) -> None:
        """
        Create a new table.
        
        Args:
            table_name: Name of the table
            columns: List of Column definitions
            
        Raises:
            ValueError: If table already exists or invalid columns
        """
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")
        
        table = Table(table_name, columns)
        self.tables[table_name] = table
    
    def insert_into(self, table_name: str, values: List[Any]) -> None:
        """
        Insert a row into a table.
        
        Args:
            table_name: Name of the table
            values: List of values in column order
            
        Raises:
            ValueError: If table doesn't exist or constraint violation
        """
        table = self._get_table(table_name)
        table.insert(values)
    
    def select_from(self, table_name: str, 
                   columns: Optional[List[str]] = None,
                   where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Select rows from a table.
        
        Args:
            table_name: Name of the table
            columns: List of column names to return (None for all)
            where: Dictionary of column=value conditions
            
        Returns:
            List of rows (as dictionaries)
            
        Raises:
            ValueError: If table doesn't exist
        """
        table = self._get_table(table_name)
        rows = table.select(where)
        
        # Filter columns if specified
        if columns:
            return [
                {col: row[col] for col in columns if col in row}
                for row in rows
            ]
        
        return rows
    
    def update_table(self, table_name: str, 
                    updates: Dict[str, Any],
                    where: Optional[Dict[str, Any]] = None) -> int:
        """
        Update rows in a table.
        
        Args:
            table_name: Name of the table
            updates: Dictionary of column=new_value
            where: Dictionary of column=value conditions
            
        Returns:
            Number of rows updated
            
        Raises:
            ValueError: If table doesn't exist or constraint violation
        """
        table = self._get_table(table_name)
        return table.update(updates, where)
    
    def delete_from(self, table_name: str, 
                   where: Optional[Dict[str, Any]] = None) -> int:
        """
        Delete rows from a table.
        
        Args:
            table_name: Name of the table
            where: Dictionary of column=value conditions
            
        Returns:
            Number of rows deleted
            
        Raises:
            ValueError: If table doesn't exist
        """
        table = self._get_table(table_name)
        return table.delete(where)
    
    def drop_table(self, table_name: str) -> None:
        """
        Drop a table from the database.
        
        Args:
            table_name: Name of the table to drop
            
        Raises:
            ValueError: If table doesn't exist
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' doesn't exist")
        
        del self.tables[table_name]
    
    def _get_table(self, table_name: str) -> Table:
        """
        Get a table by name.
        
        Args:
            table_name: Name of the table
            
        Returns:
            The Table object
            
        Raises:
            ValueError: If table doesn't exist
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' doesn't exist")
        
        return self.tables[table_name]
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about all tables.
        
        Returns:
            Dictionary with table names and row counts
        """
        return {
            name: {
                "columns": [str(col) for col in table.columns],
                "row_count": len(table.rows)
            }
            for name, table in self.tables.items()
        }
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        table_info = ", ".join(
            f"{name}({len(table.rows)} rows)" 
            for name, table in self.tables.items()
        )
        return f"DatabaseEngine(tables={len(self.tables)}: {table_info})"