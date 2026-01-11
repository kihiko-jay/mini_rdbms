"""
Read-Eval-Print Loop (REPL) for the mini RDBMS.

Interactive command-line interface for database operations.
"""

import sys
import os
from typing import Optional

# Try different import approaches
try:
    # Try absolute import first
    from db.engine import DatabaseEngine
    from db.parser import QueryParser
except ImportError:
    # Fall back to relative import
    try:
        from .engine import DatabaseEngine
        from .parser import QueryParser
    except ImportError:
        # Last resort: add current directory to path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        sys.path.insert(0, parent_dir)
        from db.engine import DatabaseEngine
        from db.parser import QueryParser


class DatabaseREPL:
    """
    Interactive REPL for database operations.
    
    Provides a simple command-line interface similar to SQLite.
    """
    
    def __init__(self):
        """Initialize the REPL with a fresh database engine."""
        self.engine = DatabaseEngine()
        self.running = False
        self._init_commands()
    
    def _init_commands(self):
        """Initialize non-SQL commands."""
        self.commands = {
            ".tables": self._list_tables,
            ".exit": self._exit,
            ".help": self._show_help,
            ".quit": self._exit,
        }
    
    def run(self):
        """Start the REPL."""
        self.running = True
        print("Mini RDBMS - Interactive Shell")
        print("Type SQL commands or .help for special commands")
        print()
        
        while self.running:
            try:
                # Read
                line = input("db> ").strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Eval and Print
                self._execute_command(line)
                
            except KeyboardInterrupt:
                print("\nInterrupted. Use .exit to quit.")
            except EOFError:
                print()
                self._exit()
            except Exception as e:
                print(f"Error: {e}")
    
    def _execute_command(self, command: str):
        """
        Execute a command (SQL or special).
        
        Args:
            command: Command string
        """
        # Check for special commands
        if command in self.commands:
            self.commands[command]()
            return
        
        # Otherwise, parse as SQL
        try:
            parsed = QueryParser.parse_query(command)
            result = self._execute_sql(parsed)
            
            if result is not None:
                if isinstance(result, list):
                    self._display_results(result)
                else:
                    print(result)
                    
        except Exception as e:
            print(f"SQL Error: {e}")
    
    def _execute_sql(self, parsed: dict):
        """
        Execute a parsed SQL command.
        
        Args:
            parsed: Parsed command from QueryParser
            
        Returns:
            Execution result (varies by command type)
            
        Raises:
            ValueError: For invalid operations
        """
        cmd_type = parsed["type"]
        
        if cmd_type == "CREATE_TABLE":
            self.engine.create_table(
                parsed["table_name"],
                parsed["columns"]
            )
            return f"Table '{parsed['table_name']}' created successfully."
        
        elif cmd_type == "INSERT":
            self.engine.insert_into(
                parsed["table_name"],
                parsed["values"]
            )
            return f"1 row inserted into '{parsed['table_name']}'."
        
        elif cmd_type == "SELECT":
            results = self.engine.select_from(
                parsed["table_name"],
                parsed["columns"],
                parsed["where"]
            )
            return results
        
        elif cmd_type == "UPDATE":
            count = self.engine.update_table(
                parsed["table_name"],
                parsed["updates"],
                parsed["where"]
            )
            return f"{count} row(s) updated in '{parsed['table_name']}'."
        
        elif cmd_type == "DELETE":
            count = self.engine.delete_from(
                parsed["table_name"],
                parsed["where"]
            )
            return f"{count} row(s) deleted from '{parsed['table_name']}'."
        
        else:
            raise ValueError(f"Unknown command type: {cmd_type}")
    
    def _display_results(self, results: list):
        """
        Display query results in a simple table format.
        
        Args:
            results: List of dictionaries (rows)
        """
        if not results:
            print("No rows found.")
            return
        
        # Get all column names
        columns = list(results[0].keys())
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            # Width is at least column name length
            max_len = len(str(col))
            
            # Check data in this column
            for row in results:
                val_len = len(str(row.get(col, "")))
                if val_len > max_len:
                    max_len = val_len
            
            col_widths[col] = max_len + 2  # Add padding
        
        # Print header
        header = " | ".join(
            str(col).ljust(col_widths[col]) 
            for col in columns
        )
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in results:
            row_str = " | ".join(
                str(row.get(col, "")).ljust(col_widths[col])
                for col in columns
            )
            print(row_str)
        
        print(f"\n({len(results)} row(s))")
    
    def _list_tables(self):
        """List all tables in the database."""
        info = self.engine.get_table_info()
        
        if not info:
            print("No tables in database.")
            return
        
        print("Tables:")
        for table_name, table_info in info.items():
            print(f"  {table_name}: {table_info['row_count']} rows")
            for col in table_info['columns']:
                print(f"    {col}")
    
    def _exit(self):
        """Exit the REPL."""
        print("Goodbye!")
        self.running = False
    
    def _show_help(self):
        """Show help message."""
        print("SQL Commands:")
        print("  CREATE TABLE name (col1 TYPE CONSTRAINT, ...)")
        print("  INSERT INTO table VALUES (val1, val2, ...)")
        print("  SELECT * FROM table [WHERE col=val]")
        print("  SELECT col1, col2 FROM table [WHERE col=val]")
        print("  UPDATE table SET col=val [WHERE conditions]")
        print("  DELETE FROM table [WHERE conditions]")
        print()
        print("Special Commands:")
        print("  .tables    - List all tables")
        print("  .exit/.quit - Exit the shell")
        print("  .help      - Show this help")


def main():
    """Main entry point for the REPL."""
    repl = DatabaseREPL()
    repl.run()


if __name__ == "__main__":
    main()