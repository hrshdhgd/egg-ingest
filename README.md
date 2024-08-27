# kegg-ingest CLI

`kegg-ingest` is a command line interface for interacting with the KEGG database. This tool allows you to fetch, process, and manage data from KEGG. All ingests are saved in a `duckdb` database and also exported as a tsv file.

## Installation

To install `kegg-ingest`, use pip:

```sh
pip install kegg-ingest
```

## Commands

### `get`

Fetch and process data from KEGG.

**Usage:**

```sh
kegg-ingest get --db <database> [--batch-size <size>] [--use-kegg/--no-use-kegg] [--output <file>]
```

**Options:**

- `--db`: KEGG Database to ingest (required).
- `--batch-size, -b`: Batch size for processing (default: 10, max: 10).
- `--use-kegg/--no-use-kegg`: Use KEGG API to fetch data (default: True). Alternatively uses [`bioservices`](https://github.com/cokelaer/bioservices)
- `--output, -o`: Output file to write to (tsv format).

**Example:**

```sh
kegg-ingest get --db pathway --batch-size 5 --use-kegg --output output.tsv
```

### `clear-db`

Clear the entire database.

**Usage:**

```sh
kegg-ingest clear-db
```

### `drop`

Drop a specific table from the database.

**Usage:**

```sh
kegg-ingest drop <table_name>
```

**Arguments:**

- `table_name`: Name of the table to drop.

**Example:**

```sh
kegg-ingest drop get_pathway
```

### `preview`

Show the contents of a table.

**Usage:**

```sh
kegg-ingest preview <table_name> [--limit <number>]
```

**Arguments:**

- `table_name`: Name of the table to preview.

**Options:**

- `--limit`: Number of rows to preview (default: 5).

**Example:**

```sh
kegg-ingest preview get_pathway --limit 10
```

### `overview`

Print an overview of the database.

**Usage:**

```sh
kegg-ingest overview
```

### `query`

Run a query on the database.

**Usage:**

```sh
kegg-ingest query <query_text>
```

**Arguments:**

- `query_text`: SQL query to run.

**Example:**

```sh
kegg-ingest query "SELECT * FROM get_pathway WHERE description LIKE '%metabolism%'"
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.


---
# Acknowledgements

This [cookiecutter](https://cookiecutter.readthedocs.io/en/stable/README.html) project was developed from the [monarch-project-template](https://github.com/monarch-initiative/monarch-project-template) template and will be kept up-to-date using [cruft](https://cruft.github.io/cruft/).
