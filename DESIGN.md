Mini RDBMS: Design Document
Overview

A minimal in-memory relational database management system (RDBMS) built in Python to demonstrate core database and systems design concepts under tight time constraints. The system focuses on correctness, clarity, and explicit trade-offs rather than production-level completeness.

System Architecture
Core Components

Column (column.py)

Represents a single column definition

Stores column name, data type (INT, TEXT), and constraints (PRIMARY KEY, UNIQUE)

Performs basic type validation

Table (table.py)

Manages rows of data for a single table

Stores column definitions and row data

Enforces constraints at insert and update time

Implements basic CRUD operations

Index (index.py)

Hash-based indexes implemented using Python dictionaries

Indexes only PRIMARY KEY and UNIQUE columns

Provides O(1) average-case lookups for indexed columns

Maintains index consistency during insert, update, and delete operations

Engine (engine.py)

Central coordinator for database operations

Maintains a table catalog (table_name → Table)

Routes parsed operations to the appropriate table

Orchestrates execution flow without transactional semantics

Parser (parser.py)

Lightweight SQL-like command parser

Uses basic string splitting and pattern matching

Converts SQL-like commands into simple structured operations for execution

Returns clear error messages for malformed or unsupported queries

REPL (repl.py)

Read–Eval–Print Loop interface

Provides an interactive command-line environment

Accepts SQL-like commands, executes them, and displays results or errors

Web App (web/app.py)

Minimal Flask application

Demonstrates real usage of the custom database engine

Exposes simple REST-style endpoints for CRUD operations

Exists solely for demonstration, not as a production API

Design Decisions & Trade-offs
1. Data Storage

Choice: In-memory Python lists and dictionaries

Trade-off: Fast access but volatile (data is lost on restart)

Reason: Keeps the focus on relational concepts rather than persistence mechanisms

2. Indexing Strategy

Choice: Hash-based indexes using Python dictionaries

Trade-offs:

Pros: O(1) average-case lookups, simple implementation

Cons: No range queries; relies on Python’s hashing behavior

Reason: Satisfies performance needs for key-based access while remaining easy to reason about

3. SQL Parsing

Choice: Custom SQL-like parser using basic string operations

Trade-offs:

Pros: No external dependencies, transparent behavior

Cons: Limited SQL support, no complex grammar handling

Reason: Demonstrates understanding of query parsing without unnecessary complexity

4. Concurrency & Transactions

Choice: Single-threaded execution with no transaction support

Trade-off: Simple but not safe for concurrent access

Reason: Out of scope for the challenge; focus placed on core relational behavior

5. Data Types

Choice: Support for INT and TEXT only

Trade-off: Limited expressiveness

Reason: Reduces implementation complexity while demonstrating type awareness

Constraints Implementation
PRIMARY KEY

Enforced during insert and update operations

Must be unique and non-null (enforced at application level)

Automatically indexed using a hash index

UNIQUE

Enforced during insert and update operations

Allows multiple null values (simplified behavior)

Automatically indexed using a hash index

Performance Considerations
Time Complexity

Insert: O(1) average-case with hash index update

Select by indexed column: O(1) average-case

Select by non-indexed column: O(n) full table scan

Update/Delete by indexed column: O(1) lookup + O(1) index update

Memory Usage

Each row stored as a Python dictionary

Indexes store references to row objects rather than copies

No explicit memory limits enforced

Limitations

No Persistence: Data is lost on process restart

No Concurrency: Single-threaded execution only

No Transactions: No commit, rollback, or isolation guarantees

Limited SQL Support: Basic CRUD operations only

No Query Optimization: Linear scans for non-indexed queries

No Security: No authentication or authorization mechanisms

Limited Validation: Only basic INT/TEXT type checking and constraint enforcement

Future Extensions

Disk-based persistence with index rebuilding on startup

Basic transaction support (BEGIN / COMMIT / ROLLBACK)

Additional data types (FLOAT, BOOLEAN, DATE)

Simple JOIN support using nested-loop joins

Basic query planning and cost-based optimization

Concurrency control via simple locking mechanism