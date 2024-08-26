"""Command line interface for kegg-ingest."""

import logging

import click

from kegg_ingest import __version__
from kegg_ingest.main import LINKS_MAP, export, get_table, parse_response, run_query
from kegg_ingest.utils import drop_table, empty_db, log_table_head, print_database_overview

__all__ = [
    "main",
]

logger = logging.getLogger(__name__)

db_option = click.option("--db", help="Database to use.", type=click.Choice(LINKS_MAP.keys()), required=True)
output_option = click.option("--output", "-o", help="Output file to write to.")
batch_option = click.option(
    "--batch-size", "-b", default=10, type=click.IntRange(0, 10), help="Batch size for processing (max 10)."
)
use_kegg_option = click.option(
    "--use-kegg/--no-use-kegg", is_flag=True, default=True, help="Use KEGG API to fetch data."
)

COLUMN_MAP = {
    "pathway": ["pathway_id", "description"],
    "module": ["module_id", "description"],
    "ko": ["ko_id", "description"],
    "ec": ["ec_id", "description"],
    "rn": ["rn_id", "description"],
    "cpd": ["cpd_id", "description"],
}


@click.group()
@click.option("-v", "--verbose", count=True)
@click.option("-q", "--quiet")
@click.version_option(__version__)
def main(verbose: int, quiet: bool):
    """
    CLI for kegg-ingest.

    :param verbose: Verbosity while running.
    :param quiet: Boolean to be quiet or verbose.
    """
    if verbose >= 2:
        logger.setLevel(level=logging.DEBUG)
    elif verbose == 1:
        logger.setLevel(level=logging.INFO)
    else:
        logger.setLevel(level=logging.WARNING)
    if quiet:
        logger.setLevel(level=logging.ERROR)


@main.command()
@db_option
@batch_option
@use_kegg_option
@output_option
def get(db: str, batch_size: int, use_kegg: bool, output: str = None):
    """Run the kegg-ingest's demo command."""
    table_name = parse_response(COLUMN_MAP.get(db, ["id", "name"]), "list", db)
    # all_tables = {db: table_name}
    # for item in LINKS_MAP.get(db):
    #     all_tables[item] = parse_response(COLUMN_MAP.get(item, ["id", "name"]), "list", item)

    get_table_name = get_table(table_name, use_kegg, batch_size)
    # pp_table_name = post_process_table(get_table_name)
    export(get_table_name, output)


@main.command()
def clear_db():
    """Clear the database."""
    empty_db()


@main.command()
@click.argument("table_name")
def drop(table_name: str):
    """Drop a table from the database."""
    drop_table(table_name)


@main.command()
@click.argument("table_name")
@click.option("--limit", default=5, help="Number of rows to preview.")
def preview(table_name: str, limit: int):
    """Show the contents of a table."""
    log_table_head(table_name, limit=limit)


@main.command()
def overview():
    """Print an overview of the database."""
    print_database_overview()


@main.command()
@click.argument("query_text")
def query(query_text: str):
    """Run a query on the database."""
    run_query(query_text)


if __name__ == "__main__":
    main()
