"""
Create the database tables
"""
import click
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import Base


@click.command()
def cli():
    """
    Create the database tables for web UI authentication
    """
    print("Connecting...")
    engine = connect_db()
    print("Creating database tables")
    Base.metadata.create_all(engine)
    print("Done")


if __name__ == "__main__":
    cli()
