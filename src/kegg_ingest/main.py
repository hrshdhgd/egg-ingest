"""Main python file."""

import csv
import requests_cache
import urllib3
from io import TextIOWrapper
import duckdb

LINKS_MAP = {
    "rn": ["cpd", "ko", "ec","module", "pathway"],
    "cpd": ["ko", "ec", "module", "pathway", "rn"],
    "ko": ["ec", "module", "pathway", "rn", "cpd"],
    "ec": ["module", "pathway", "rn", "cpd", "ko"],
    "module": ["pathway", "rn", "cpd", "ko", "ec"],
    "pathway": ["rn", "cpd", "ko", "ec", "module"],
}

KEGG_URL = 'http://rest.kegg.jp/'


def get_db_connection(db_path='kegg_data.db'):
    return duckdb.connect(database=db_path)

def has_digit(string):
    return any(char.isdigit() for char in string)

def empty_db(db_path='kegg_data.db'):
    conn = get_db_connection(db_path)
    
    # Get the list of all tables in the database
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main';").fetchall()
    
    # Drop each table
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table[0]};")
    
    print(f"All tables in '{db_path}' have been dropped.")

def parse_response(cols, *args):
    global KEGG_URL
    for arg in args:
        KEGG_URL += arg + '/'

    # Enable caching
    requests_cache.install_cache('kegg_cache')

    http = urllib3.PoolManager()
    pathwayResponse = http.request('GET', KEGG_URL, preload_content=False)
    pathwayResponse.auto_close = False

    table_name = args[-1]
    cols_type = [f"{col} STRING" for col in cols]
    create_table_query = f"CREATE TABLE {table_name} ({', '.join(cols_type)});"
    conn = get_db_connection()
    
    # Check if the table already exists
    table_exists_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}';"
    table_exists = conn.execute(table_exists_query).fetchone()[0]

    if table_exists:
        # Table exists, do nothing and return
        return table_name

    
    conn.execute(create_table_query)

    insert_query = f"INSERT INTO {table_name} VALUES (?, ?);"

    for line in TextIOWrapper(pathwayResponse):
        row = line.strip().split('\t')
        conn.execute(insert_query, row) if len(row) == len(cols) else None

    # Create an index on the first column (assuming it's the primary key or unique identifier)
    index_query = f"CREATE INDEX idx_{table_name}_{cols[0]} ON {table_name}({cols[0]});"
    conn.execute(index_query)
    return table_name

def fetch_kegg_data(item, http):
    new_kegg_url = KEGG_URL + 'get/' + item
    pathway_response = http.request('GET', new_kegg_url, preload_content=False)
    pathway_response.auto_close = False
    return pathway_response

def process_kegg_response(response):
    dictionary = {}
    last_key = ''
    non_column_chars = ['-', ' ', ';']

    for line in TextIOWrapper(response):
        line_elements = line.split('  ')
        list_of_elements = [x.strip() for x in line_elements if x]

        if list_of_elements[0].isupper() \
        and not has_digit(list_of_elements[0]) \
        and not any(map(list_of_elements[0].__contains__, non_column_chars)) \
        and len(list_of_elements) > 1 \
        and len(list_of_elements[0]) > 3:
            
            last_key = list_of_elements[0]
                
            if last_key == 'ENZYME':
                dictionary[last_key] = ' | '.join(list_of_elements[1:])
            elif last_key in dictionary.keys():
                dictionary[last_key] += (' | '+'-'.join(list_of_elements[1:]))
            else:
                dictionary[last_key] = ' '.join(list_of_elements[1:])
        else:
            if last_key == 'COMMENT':
                dictionary[last_key] += (' '+' '.join(list_of_elements))
            else:
                dictionary[last_key] += (' | '+'-'.join(list_of_elements))

        dictionary[last_key] = dictionary[last_key].replace(' | ///', '')
    
    return dictionary


def make_dataframe(table_name, output_file= None):
    """Make a dataframe from a table in the database."""
    http = urllib3.PoolManager()
    conn = get_db_connection()
    if output_file is None:
        output_file = f"{table_name}_responses.tsv"

    # Fetch the table schema to get the column names
    schema_query = f"DESCRIBE {table_name};"
    schema_result = conn.execute(schema_query).fetchall()

    # Assuming the table has exactly two columns
    id_col_name = schema_result[0][0]
    second_col_name = schema_result[1][0]

    # Fetch the table data
    query = f"SELECT {id_col_name}, {second_col_name} FROM {table_name};"
    original_data = conn.execute(query).fetchall()

    # Initialize an empty list to store the new rows
    new_rows = []
    
    # Iterate over each row in the original dataframe
    for row in original_data:
        response = fetch_kegg_data(row[0], http)
        new_row = (response, row[1])
        new_rows.append(new_row)
    
    # Create a new table with the responses and the second column
    new_table_name = f"{table_name}_with_responses"
    create_table_query = f"""
    CREATE TABLE {new_table_name} (
        Response TEXT,
        {second_col_name} TEXT
    );
    """
    conn.execute(create_table_query)
    
    # Insert the new rows into the new table
    insert_query = f"INSERT INTO {new_table_name} VALUES (?, ?);"
    conn.executemany(insert_query, new_rows)
    # Write the new rows to a TSV file
    with open(output_file, 'w', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        # Write the header
        writer.writerow(['Response', second_col_name])
        # Write the data rows
        writer.writerows(new_rows)
    
    return new_table_name

if __name__ == "__main__":
    get_db_connection()
