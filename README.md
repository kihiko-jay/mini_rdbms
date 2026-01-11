# Mini RDBMS (Python)

A minimal in-memory relational database management system built in Python as part of a technical hiring challenge.

---

## Features
- SQL-like syntax (CREATE, INSERT, SELECT, UPDATE, DELETE)
- Primary key and UNIQUE constraints
- Hash-based indexing
- Interactive REPL
- Demo Flask web application using the custom database engine

---

## Why This Project
The goal of this project is to demonstrate systems thinking, data modeling, and clear trade-off awareness -not to build a production-ready database.

---

## How to Run

### REPL
```bash
python launch.py
Web App
bash
Copy code
python web/app.py
Then visit:

arduino
Copy code
http://localhost:5000
Limitations
In-memory only (no persistence)

No transactions or concurrency

Limited SQL support

Design
Architectural decisions, trade-offs, and limitations are documented in DESIGN.md.

Use of AI Tools
AI tools (ChatGPT, DeepSeek, and GitHub Copilot) were used for guidance, design discussion, and code review.
All design decisions and final implementation were done and fully understood by  the author(James Kihiko).