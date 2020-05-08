"""
Launch a shell with an active database session to perform queries
against the database
"""
import click
import readline  # Import required for command history
import code

from passari_workflow.db.connection import connect_db
from passari_workflow.db import DBSession
from passari_workflow.db.models import *


@click.command()
def cli():
    """
    Start a REPL session with active DB session and DB models
    """
    connect_db()
    db = DBSession()

    console = code.InteractiveConsole(locals={"db": db})
    console.runsource("from passari_workflow.db.models import *")
    console.interact(
        "SQLAlchemy database session (`db`) and Passari models are "
        "available in this console.\n"
        "\n"
        "For example, you can run the following command:\n"
        "> non_preserved_objects = "
        "db.query(MuseumObject).filter_by(preserved=False)"
    )


if __name__ == "__main__":
    cli()
