"""Main python file."""

import csv
import logging
import time
from io import TextIOWrapper

import requests_cache
import urllib3

from kegg_ingest.utils import (
    get_db_connection,
    has_digit,
    insert_data_with_flexible_columns,
)

LINKS_MAP = {
    "rn": ["cpd", "ko", "ec", "module", "pathway"],
    "cpd": ["ko", "ec", "module", "pathway", "rn"],
    "ko": ["ec", "module", "pathway", "rn", "cpd"],
    "ec": ["module", "pathway", "rn", "cpd", "ko"],
    "module": ["pathway", "rn", "cpd", "ko", "ec"],
    "pathway": ["rn", "cpd", "ko", "ec", "module"],
}

KEGG_URL = "http://rest.kegg.jp/"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
BATCH_SIZE = 10


def parse_response(cols, *args):
    """Parse the KEGG response and create a table in the database."""
    global KEGG_URL
    url = KEGG_URL
    for arg in args:
        url += arg + "/"

    # Enable caching
    requests_cache.install_cache("kegg_cache")

    http = urllib3.PoolManager()
    pathwayResponse = http.request("GET", url, preload_content=False)
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
        logging.info(f"Table '{table_name}' already exists.")
        return table_name

    conn.execute(create_table_query)

    insert_query = f"INSERT INTO {table_name} VALUES (?, ?);"

    for line in TextIOWrapper(pathwayResponse):
        row = line.strip().split("\t")
        conn.execute(insert_query, row) if len(row) == len(cols) else None

    # Create an index on the first column (assuming it's the primary key or unique identifier)
    index_query = f"CREATE INDEX idx_{table_name}_{cols[0]} ON {table_name}({cols[0]});"
    conn.execute(index_query)
    conn.close()
    logging.info(f"Table '{table_name}' has been created.")
    return table_name


def process_kegg_response(response):
    """Process the KEGG response."""
    dictionary = {}
    last_key = ""
    non_column_chars = ["-", " ", ";"]

    for line in TextIOWrapper(response):
        if line.startswith("///"):
            yield dictionary
            dictionary = {}
            last_key = ""
            continue
        line_elements = line.split("  ")
        list_of_elements = [x.strip() for x in line_elements if x]

        if list_of_elements[0].split(" ")[0].isupper():
            list_of_elements = list_of_elements[0].split(" ") + list_of_elements[1:]

        if (
            list_of_elements[0].isupper()
            and not has_digit(list_of_elements[0])
            and not any(map(list_of_elements[0].__contains__, non_column_chars))
            and len(list_of_elements) > 1
            and len(list_of_elements[0]) > 3
            and not list_of_elements[0].endswith(":")
        ):

            last_key = list_of_elements[0]

            if last_key == "ENZYME":
                dictionary[last_key] = " | ".join(list_of_elements[1:])
            elif last_key in dictionary.keys():
                dictionary[last_key] += " | " + "-".join(list_of_elements[1:])
            else:
                dictionary[last_key] = " ".join(list_of_elements[1:])
        else:
            if last_key == "":
                continue
            elif last_key == "COMMENT":
                dictionary[last_key] += " " + " ".join(list_of_elements)
            else:
                dictionary[last_key] += " | " + "-".join(list_of_elements)

        dictionary[last_key] = dictionary[last_key].replace(" | ///", "")

        yield dictionary


def fetch_kegg_data(item, http, retries=5, backoff_factor=0.3):
    """Fetch KEGG data for a given item."""
    new_kegg_url = KEGG_URL + "get/" + item
    attempt = 0
    
    while attempt < retries:
        try:
            pathway_response = http.request("GET", new_kegg_url, preload_content=False)
            
            if pathway_response.status == 200:
                pathway_response.auto_close = False
                yield from process_kegg_response(pathway_response)
                return
            
            elif pathway_response.status in {403, 404}:
                error_messages = {
                    403: "Access forbidden: Check if the URL is correct and if you have the necessary permissions.",
                    404: "Not found: Check if the URL is correct."
                }
                print(error_messages[pathway_response.status])
                if pathway_response.status == 403:
                    print("Sleeping for 500 seconds before retrying.")
                    time.sleep(500)
                return
            
            else:
                print(f"An error occurred: {pathway_response.status}")
                return
        
        except (urllib3.exceptions.IncompleteRead, urllib3.exceptions.NewConnectionError) as e:
            attempt += 1
            if attempt >= retries:
                print(f"Failed after {retries} attempts: {e}")
                raise
            else:
                print(f"Attempt {attempt} failed: {e}. Retrying in {backoff_factor * (2 ** attempt)} seconds...")
                time.sleep(backoff_factor * (2 ** attempt))
        
        except urllib3.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            return
        
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return


def get_table(table_name, batch_size:int = BATCH_SIZE):
    """Make a dataframe from a table in the database."""
    http = urllib3.PoolManager()
    conn = get_db_connection()
    columns = None

    # Create a new table with the responses and the second column
    try:
        new_table_name = f"get_{table_name}"
        logging.info(f"Fetching data for table '{table_name}'.")
        # Check if the table already exists
        table_exists_query = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{new_table_name}';
        """
        table_exists = conn.execute(table_exists_query).fetchone()[0]

        if not table_exists:
            # Fetch the table schema to get the column names
            schema_query = f"DESCRIBE {table_name};"
            schema_result = conn.execute(schema_query).fetchall()

            # Assuming the table has exactly two columns
            id_col_name, _second_col_name = schema_result[0][0], schema_result[1][0]

            # Fetch the table data
            query = f"SELECT {id_col_name} FROM {table_name};"
            original_data = conn.execute(query).fetchall()
            list_of_lists = [
                "+".join(item[0] for item in original_data[i : i + batch_size])
                for i in range(0, len(original_data), batch_size)
            ]
            # Create new rows with fetched KEGG data
            table_created = False
            data_batch = []
            for batch in list_of_lists:
                for response in fetch_kegg_data(batch, http):
                    if not table_created:
                        # Extract columns and create the table
                        columns = ", ".join([f"{col.lower()} VARCHAR" for col in response.keys()])
                        create_table_query = f"CREATE TABLE {new_table_name} ({columns})"
                        logging.info(f"Query: {create_table_query}")
                        conn.execute(create_table_query)
                        table_created = True
                    
                    data_batch.append(response)
                    
                insert_data_with_flexible_columns(conn, new_table_name, data_batch)

            conn.commit()
        else:
            logging.info(f"Table '{new_table_name}' already exists.")

        temp_table = f"{new_table_name}_temp"
        conn.execute(f"""
        CREATE TABLE {temp_table} AS
        SELECT DISTINCT * FROM {new_table_name};
        """)
        conn.execute(f"DROP TABLE {new_table_name};")
        conn.execute(f"ALTER TABLE {temp_table} RENAME TO {new_table_name};")

        return new_table_name
    finally:
        conn.close()


def post_process_table(table_name: str):
    """Post-process the table to split multi-value columns."""
    # TODO: Implement this function based on table_name passed.

    pass


def export(table_name: str, output: str = None, format: str = "tsv"):
    """Export a table to a file."""
    conn = get_db_connection()

    try:
        # Fetch all data from the table
        query = f"SELECT * FROM {table_name};"
        results = conn.execute(query).fetchall()

        if not results:
            logging.error(f"No data found in table '{table_name}'.")
            return

        # Fetch column names
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]

    finally:
        conn.close()

    if not output:
        output = f"{table_name}.{format}"

    with open(output, "w", newline="") as file:
        writer = csv.writer(file, delimiter="\t" if format == "tsv" else ",")

        # Write the header
        writer.writerow(column_names)

        # Write the rows
        writer.writerows(results)

    logging.info(f"Table '{table_name}' has been exported to '{output}'.")


if __name__ == "__main__":
    get_db_connection()
