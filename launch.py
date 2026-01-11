#!/usr/bin/env python3
"""
Simple launcher for the Mini RDBMS REPL.
Run this from the project root directory.
"""
import sys
import os

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Now we can import
from db.repl import DatabaseREPL

if __name__ == "__main__":
    repl = DatabaseREPL()
    repl.run()