Simple In-Memory SQL Query Engine

Welcome to the Simple In-Memory SQL Engine! This is a lightweight, dependency-free Python tool designed to mimic the core behavior of a relational database. Whether you're learning how database internals work or just need a quick way to query CSVs without overhead, this engine provides a clean, "no-fuss" solution.

1. Project Overview

This engine transforms raw CSV data into a queryable in-memory database. Built entirely in Python without external libraries (no Pandas, no SQLite), it demonstrates the fundamental pipeline of data processing:

Data Ingestion: Automatically cleans whitespace and normalizes data types from your CSV files.

Parsing: Deconstructs your SQL strings into logical components (Select, From, Where).

Execution: Efficiently filters and projects data using Python's native memory management.

2. Getting Started

Prerequisites

Python 3.x: No extra pip install commands needed!

Core Files: Ensure sql_engine.py, cli.py, and your .csv files are in the same directory.

Running the Engine

1.Navigate to your project folder in the terminal.
2.Launch the interactive interface:
Bash
python cli.py
3.Start Querying! You'll see a prompt where you can begin entering commands.

3. Supported SQL Grammar

The engine is case-insensitive and normalizes table/column names to lowercase for a smoother user experience.

I. Loading Data

Before running queries, tell the engine which files to read.

Syntax: LOAD <filename.csv> AS <table_name>
Example: LOAD employees.csv AS staff

II. Selecting Data (Projection)

Choose exactly what you want to see.

The Big Picture: SELECT * FROM staff

Specifics: SELECT name, department FROM staff

III. Filtering (The WHERE Clause)

Narrow down your results using standard comparison operators (=, !=, >, <, >=, <=).

Numbers: SELECT name FROM staff WHERE age > 30

Strings: SELECT * FROM staff WHERE department = 'Sales'

IV. Simple Analytics (Aggregation)

Quickly count your records.

SELECT COUNT(*) FROM staff
SELECT COUNT(*) FROM staff WHERE department = 'IT'

4. Sample Datasets

The project comes with two ready-to-use files:
employees.csv
departments.csv

5. Built-in Error Handling

Don't worry about breaking things! The engine is designed to catch and explain common hiccups:

Table Not Found: Occurs if you forget to LOAD a file first.

Column Not Found: Triggered if a column name is misspelled or missing from the CSV header.

Syntax Error: Provides feedback if keywords like SELECT or FROM are missing.

6. Structre of Simple In-Memory SQL Query Engine

sql-query-engine/
├── cli.py              # The entry point (run this to start)
├── sql_engine.py       # Core logic and SQL parsing
├── employees.csv       # Dataset: Personnel records
├── departments.csv     # Dataset: Department records
└── README.md           # Documentation
