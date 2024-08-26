"""Utility functions for KEGG ingestion."""


from pprint import pprint
from typing import List, Dict
import duckdb



def get_db_connection(db_path="kegg_data.db"):
    """Get a connection to the database."""
    return duckdb.connect(database=db_path)


def has_digit(string):
    """Check if a string has a digit."""
    return any(char.isdigit() for char in string)


def empty_db(db_path="kegg_data.db"):
    """Empty the database by dropping all tables."""
    conn = get_db_connection(db_path)

    # Get the list of all tables in the database
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main';").fetchall()

    # Drop each table
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table[0]};")

    pprint(f"All tables in '{db_path}' have been dropped.")


def drop_table(table_name):
    """Drop a specific table from the database."""
    conn = get_db_connection()

    # Check if the table exists
    table_exists_query = f"""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = '{table_name}';
    """
    table_exists = conn.execute(table_exists_query).fetchone()[0]

    if table_exists:
        # Drop the table if it exists
        drop_table_query = f"DROP TABLE {table_name};"
        conn.execute(drop_table_query)
        pprint(f"Table '{table_name}' has been dropped.")
    else:
        pprint(f"Table '{table_name}' does not exist.")


def print_database_overview():
    """Print a bird's eye view of the database: schema names, table names, and column names."""
    conn = get_db_connection()

    # Query to get all tables and their columns
    query = """
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    ORDER BY table_schema, table_name, ordinal_position;
    """

    results = conn.execute(query).fetchall()

    if not results:
        pprint("No tables found in the database.")
        return

    current_schema = None
    current_table = None
    for schema_name, table_name, column_name in results:
        if schema_name != current_schema:
            if current_schema is not None:
                pprint("\n")
            current_schema = schema_name
            pprint(f"## Schema: {schema_name}")

        if table_name != current_table:
            if current_table is not None:
                pprint("\n")
            current_table = table_name

            # Get the row count for the current table
            row_count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
            row_count = conn.execute(row_count_query).fetchone()[0]

            pprint(f"### Table: {table_name} (Rows: {row_count})")

        pprint(f"- Column: {column_name}")

    conn.close()


def log_table_head(table_name: str, limit: int = 5):
    """Log the first few rows of a table."""
    conn = get_db_connection()
    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit};"
        results = conn.execute(query).fetchdf()
        # Fetch column names
        columns = [desc[0] for desc in conn.description]

        # Log the results
        pprint(f"First {limit} rows from table '{table_name}':")
        pprint(results.head(limit))

    except Exception as e:
        pprint(f"Error: {e}")
    finally:
        conn.close()


def add_new_columns_if_needed(conn, table_name, columns):
    """Add new columns to the table if they do not exist."""
    existing_columns_query = f"PRAGMA table_info({table_name})"
    existing_columns_info = conn.execute(existing_columns_query).fetchall()
    existing_columns = {col[1].lower() for col in existing_columns_info}
    new_columns = [col for col in columns if col.lower() not in existing_columns if len(col) > 3]

    for col in new_columns:
        col = col.lower()
        alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT DEFAULT NULL"
        conn.execute(alter_table_query)
        pprint(f"Added new column '{col}' to table '{table_name}'.")

def clean_value(value):
    """Clean the value by stripping leading/trailing whitespace and replacing multiple spaces/tabs."""
    if isinstance(value, str):
        return ' '.join(value.split())
    return value

def insert_data_with_flexible_columns(conn: duckdb.DuckDBPyConnection, table_name: str, data_batch: List[Dict]):
    """Insert data into the table, adding new columns if necessary."""
    if not data_batch:
        return  # No data to insert
    
    # Collect all unique column names from the batch
    all_columns = set()
    for response in data_batch:
        all_columns.update(response.keys())
    
    # Check and add new columns if needed
    add_new_columns_if_needed(conn, table_name, list(all_columns))
    
    # Prepare the insert query
    keys = ", ".join(all_columns).lower()
    placeholders = ", ".join(["?" for _ in all_columns])
    insert_query = f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders})"
    
    # Insert each row into the table
    for response in data_batch:
        potential_values = [clean_value(response.get(col, None)) for col in all_columns]
        conn.execute(insert_query, potential_values)

    


# def parse_data(data):
#     """Parse KEGG data into a dictionary."""
#     # Initialize the result dictionary
#     result = {"columns": list(data.keys()), "rows": []}
#     # Get the maximum number of splits for any key
#     max_splits = max(len(value.split(" | ")) for value in data.values())
#     # Create empty rows based on the maximum number of splits
#     rows = [[] for _ in range(max_splits)]

#     # Populate the rows with split values
#     for _, val in data.items():
#         split_values = val.split(" | ")
#         last_value = split_values[-1]
#         for i in range(max_splits):
#             if i < len(split_values):
#                 rows[i].append(split_values[i])
#             else:
#                 # Repeat the last value if there are fewer splits
#                 rows[i].append(last_value)

#     # Assign the populated rows to the result dictionary
#     result["rows"] = rows
#     return result