"""Utility functions for KEGG ingestion."""


import duckdb


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
