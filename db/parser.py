"""
SQL-like command parser for the mini RDBMS.

Parses simple SQL commands into structured operations.
"""

import re
from typing import Any, Dict, List, Optional, Tuple
from .column import Column, DataType


class QueryParser:
    """
    Parser for SQL-like commands.
    
    Supports limited SQL syntax:
    - CREATE TABLE
    - INSERT INTO
    - SELECT
    - UPDATE
    - DELETE FROM
    """
    
    @staticmethod
    def parse_create_table(query: str) -> Tuple[str, List[Column]]:
        """
        Parse CREATE TABLE command.
        
        Format: CREATE TABLE table_name (col1 TYPE CONSTRAINT, ...)
        
        Args:
            query: SQL CREATE TABLE command
            
        Returns:
            Tuple of (table_name, list_of_columns)
            
        Raises:
            ValueError: If syntax is invalid
        """
        # Simple regex to match CREATE TABLE pattern
        pattern = r'CREATE TABLE\s+(\w+)\s*\((.*)\)'
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid CREATE TABLE syntax. "
                           "Expected: CREATE TABLE name (col1 TYPE CONSTRAINT, ...)")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        columns = []
        col_defs = [col.strip() for col in columns_str.split(',')]
        
        for col_def in col_defs:
            if not col_def:
                continue
            
            # Parse column definition
            parts = col_def.strip().split()
            if len(parts) < 2:
                raise ValueError(f"Invalid column definition: {col_def}")
            
            col_name = parts[0]
            data_type_str = parts[1].upper()
            
            # Map string to DataType enum
            if data_type_str == "INT":
                data_type = DataType.INT
            elif data_type_str == "TEXT":
                data_type = DataType.TEXT
            else:
                raise ValueError(f"Unsupported data type: {data_type_str}")
            
            # Parse constraints
            is_primary_key = "PRIMARY KEY" in col_def.upper()
            is_unique = "UNIQUE" in col_def.upper() or is_primary_key
            
            column = Column(
                name=col_name,
                data_type=data_type,
                is_primary_key=is_primary_key,
                is_unique=is_unique
            )
            columns.append(column)
        
        return table_name, columns
    
    @staticmethod
    def parse_insert(query: str) -> Tuple[str, List[Any]]:
        """
        Parse INSERT INTO command.
        
        Format: INSERT INTO table_name VALUES (val1, val2, ...)
        
        Args:
            query: SQL INSERT command
            
        Returns:
            Tuple of (table_name, list_of_values)
            
        Raises:
            ValueError: If syntax is invalid
        """
        pattern = r'INSERT INTO\s+(\w+)\s+VALUES\s*\((.*)\)'
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid INSERT syntax. "
                           "Expected: INSERT INTO table VALUES (val1, val2, ...)")
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        # Parse values (simple split by comma)
        values = []
        for val in values_str.split(','):
            val = val.strip()
            
            # Remove quotes for TEXT values
            if val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            elif val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            
            # Convert numeric values
            if val.isdigit() or (val.startswith('-') and val[1:].isdigit()):
                val = int(val)
            
            values.append(val)
        
        return table_name, values
    
    @staticmethod
    def parse_select(query: str) -> Tuple[str, Optional[List[str]], Optional[Dict[str, Any]]]:
        """
        Parse SELECT command.
        
        Format: SELECT * FROM table_name [WHERE col=val]
               SELECT col1, col2 FROM table_name [WHERE col=val]
        
        Args:
            query: SQL SELECT command
            
        Returns:
            Tuple of (table_name, columns_list, where_conditions)
            
        Raises:
            ValueError: If syntax is invalid
        """
        # Parse basic SELECT
        select_pattern = r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
        match = re.match(select_pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid SELECT syntax. "
                           "Expected: SELECT columns FROM table [WHERE conditions]")
        
        columns_str = match.group(1).strip()
        table_name = match.group(2)
        where_str = match.group(3)
        
        # Parse columns
        if columns_str == "*":
            columns = None  # None means all columns
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parse WHERE clause
        where = None
        if where_str:
            where = QueryParser._parse_where_clause(where_str)
        
        return table_name, columns, where
    
    @staticmethod
    def parse_update(query: str) -> Tuple[str, Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Parse UPDATE command.
        
        Format: UPDATE table_name SET col1=val1, col2=val2 [WHERE conditions]
        
        Args:
            query: SQL UPDATE command
            
        Returns:
            Tuple of (table_name, updates_dict, where_conditions)
            
        Raises:
            ValueError: If syntax is invalid
        """
        pattern = r'UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?'
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid UPDATE syntax. "
                           "Expected: UPDATE table SET col=val [WHERE conditions]")
        
        table_name = match.group(1)
        set_str = match.group(2)
        where_str = match.group(3)
        
        # Parse SET clause
        updates = {}
        for assignment in set_str.split(','):
            assignment = assignment.strip()
            if '=' not in assignment:
                raise ValueError(f"Invalid assignment: {assignment}")
            
            col, val = assignment.split('=', 1)
            col = col.strip()
            val = val.strip()
            
            # Parse value
            val = QueryParser._parse_value(val)
            updates[col] = val
        
        # Parse WHERE clause
        where = None
        if where_str:
            where = QueryParser._parse_where_clause(where_str)
        
        return table_name, updates, where
    
    @staticmethod
    def parse_delete(query: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Parse DELETE FROM command.
        
        Format: DELETE FROM table_name [WHERE conditions]
        
        Args:
            query: SQL DELETE command
            
        Returns:
            Tuple of (table_name, where_conditions)
            
        Raises:
            ValueError: If syntax is invalid
        """
        pattern = r'DELETE FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid DELETE syntax. "
                           "Expected: DELETE FROM table [WHERE conditions]")
        
        table_name = match.group(1)
        where_str = match.group(2)
        
        # Parse WHERE clause
        where = None
        if where_str:
            where = QueryParser._parse_where_clause(where_str)
        
        return table_name, where
    
    @staticmethod
    def _parse_where_clause(where_str: str) -> Dict[str, Any]:
        """
        Parse WHERE clause into conditions dictionary.
        
        Currently only supports simple equality conditions.
        
        Args:
            where_str: WHERE clause string
            
        Returns:
            Dictionary of column=value conditions
            
        Raises:
            ValueError: If WHERE clause is invalid
        """
        conditions = {}
        
        # Split by AND (simple case)
        and_parts = [p.strip() for p in where_str.split('AND')]
        
        for part in and_parts:
            if '=' not in part:
                raise ValueError(f"Invalid condition: {part}")
            
            col, val = part.split('=', 1)
            col = col.strip()
            val = val.strip()
            
            # Parse value
            val = QueryParser._parse_value(val)
            conditions[col] = val
        
        return conditions
    
    @staticmethod
    def _parse_value(val_str: str) -> Any:
        """
        Parse a SQL value string into Python value.
        
        Args:
            val_str: Value string (may be quoted)
            
        Returns:
            Parsed value (int or str)
        """
        val_str = val_str.strip()
        
        # Remove quotes for strings
        if (val_str.startswith("'") and val_str.endswith("'")) or \
           (val_str.startswith('"') and val_str.endswith('"')):
            return val_str[1:-1]
        
        # Try to parse as integer
        try:
            return int(val_str)
        except ValueError:
            # Return as string
            return val_str
    
    @staticmethod
    def parse_query(query: str) -> Dict[str, Any]:
        """
        Parse any SQL command and return operation details.
        
        Args:
            query: SQL command string
            
        Returns:
            Dictionary with operation details
            
        Raises:
            ValueError: If command is not recognized
        """
        query = query.strip()
        if not query:
            raise ValueError("Empty query")
        
        query_upper = query.upper()
        
        try:
            if query_upper.startswith("CREATE TABLE"):
                table_name, columns = QueryParser.parse_create_table(query)
                return {
                    "type": "CREATE_TABLE",
                    "table_name": table_name,
                    "columns": columns
                }
            elif query_upper.startswith("INSERT INTO"):
                table_name, values = QueryParser.parse_insert(query)
                return {
                    "type": "INSERT",
                    "table_name": table_name,
                    "values": values
                }
            elif query_upper.startswith("SELECT"):
                table_name, columns, where = QueryParser.parse_select(query)
                return {
                    "type": "SELECT",
                    "table_name": table_name,
                    "columns": columns,
                    "where": where
                }
            elif query_upper.startswith("UPDATE"):
                table_name, updates, where = QueryParser.parse_update(query)
                return {
                    "type": "UPDATE",
                    "table_name": table_name,
                    "updates": updates,
                    "where": where
                }
            elif query_upper.startswith("DELETE FROM"):
                table_name, where = QueryParser.parse_delete(query)
                return {
                    "type": "DELETE",
                    "table_name": table_name,
                    "where": where
                }
            else:
                raise ValueError(f"Unsupported command: {query.split()[0]}")
        except Exception as e:
            raise ValueError(f"Failed to parse query: {str(e)}")