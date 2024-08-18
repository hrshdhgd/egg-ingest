"""Main python file."""

import csv
import logging
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


def fetch_kegg_data(item, http):
    """Fetch KEGG data for a given item."""
    new_kegg_url = KEGG_URL + "get/" + item
    pathway_response = http.request("GET", new_kegg_url, preload_content=False)
    pathway_response.auto_close = False
    yield from process_kegg_response(pathway_response)


def parse_data(data):
    """Parse KEGG data into a dictionary."""
    # Initialize the result dictionary
    result = {"columns": list(data.keys()), "rows": []}
    # Get the maximum number of splits for any key
    max_splits = max(len(value.split(" | ")) for value in data.values())
    # Create empty rows based on the maximum number of splits
    rows = [[] for _ in range(max_splits)]

    # Populate the rows with split values
    for _, val in data.items():
        split_values = val.split(" | ")
        last_value = split_values[-1]
        for i in range(max_splits):
            if i < len(split_values):
                rows[i].append(split_values[i])
            else:
                # Repeat the last value if there are fewer splits
                rows[i].append(last_value)

    # Assign the populated rows to the result dictionary
    result["rows"] = rows
    return result


def get_table(table_name):
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
            # Create new rows with fetched KEGG data
            responses = [response for row in original_data for response in fetch_kegg_data(row[0], http)]

            for idx, response in enumerate(responses):

                if idx == 0:
                    # Extract columns and create the table
                    columns = ", ".join([f"{col.lower()} VARCHAR" for col in response.keys()])
                    create_table_query = f"CREATE TABLE {new_table_name} ({columns})"
                    conn.execute(create_table_query)

                # Insert each row into the table
                insert_data_with_flexible_columns(conn, new_table_name, response)

            conn.commit()

        else:
            logging.info(f"Table '{new_table_name}' already exists.")

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
