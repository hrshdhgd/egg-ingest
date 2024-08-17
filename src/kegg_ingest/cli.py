"""Command line interface for kegg-ingest."""
import logging

import click

from kegg_ingest import __version__
from kegg_ingest.main import LINKS_MAP, empty_db, make_dataframe, parse_response

__all__ = [
    "main",
]

logger = logging.getLogger(__name__)

db_option = click.option("--db", help="Database to use.", type=click.Choice(LINKS_MAP.keys()), required=True)

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
def run(db: str):
    """Run the kegg-ingest's demo command."""

    table_name = parse_response(COLUMN_MAP.get(db, ["id", "name"]), "list", db)
    # all_tables = {db: table_name}
    # for item in LINKS_MAP.get(db):
    #     all_tables[item] = parse_response(COLUMN_MAP.get(item, ["id", "name"]), "list", item)
    make_dataframe(table_name)

@main.command()
def clear_db():
    """Clear the database."""
    empty_db()


if __name__ == "__main__":
    main()
