"""Utility functions for KEGG ingestion."""


import duckdb
from tabulate import tabulate

def get_db_connection(db_path="kegg_data.db"):
    return duckdb.connect(database=db_path)


def has_digit(string):
    return any(char.isdigit() for char in string)


def empty_db(db_path="kegg_data.db"):
    conn = get_db_connection(db_path)

    # Get the list of all tables in the database
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main';").fetchall()

    # Drop each table
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table[0]};")

    print(f"All tables in '{db_path}' have been dropped.")


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
        print(f"Table '{table_name}' has been dropped.")
    else:
        print(f"Table '{table_name}' does not exist.")


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
        print("No tables found in the database.")
        return

    current_schema = None
    current_table = None
    for schema_name, table_name, column_name in results:
        if schema_name != current_schema:
            if current_schema is not None:
                print("\n")
            current_schema = schema_name
            print(f"## Schema: {schema_name}")

        if table_name != current_table:
            if current_table is not None:
                print("\n")
            current_table = table_name

            # Get the row count for the current table
            row_count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
            row_count = conn.execute(row_count_query).fetchone()[0]

            print(f"### Table: {table_name} (Rows: {row_count})")

        print(f"- Column: {column_name}")

    conn.close()

def log_table_head(table_name:str, limit:int=5):
    conn = get_db_connection()
    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit};"
        results = conn.execute(query).fetchall()
        # Fetch column names
        columns = [desc[0] for desc in conn.description]
        
        # Log the results in a tabulated format
        print(f"First {limit} rows from table '{table_name}':")
        print(tabulate(results, headers=columns, tablefmt="grid"))
        
    except Exception as e:
        print(f"Error: {e}")    
    finally:
        conn.close()

def add_new_columns_if_needed(conn, table_name, columns):
    """Add new columns to the table if they do not exist."""
    existing_columns_query = f"PRAGMA table_info({table_name})"
    existing_columns_info = conn.execute(existing_columns_query).fetchall()
    existing_columns = {col[1].lower() for col in existing_columns_info}
    new_columns = [col for col in columns if col.lower() not in existing_columns]

    for col in new_columns:
        alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT DEFAULT NULL"
        conn.execute(alter_table_query)
        print(f"Added new column '{col}' to table '{table_name}'.")

def insert_data_with_flexible_columns(conn, table_name, parsed_data):
    """Insert data into the table, adding new columns if necessary."""
    for row in parsed_data['rows']:
        # Check and add new columns if needed
        add_new_columns_if_needed(conn, table_name, parsed_data['columns'])
        
        # Prepare the insert query
        keys = ', '.join(parsed_data['columns']).lower()
        placeholders = ', '.join(['?' for _ in parsed_data['columns']])
        insert_query = f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders})"
        
        # Insert the row into the table
        conn.execute(insert_query, row)
