"""
Test Alembic migrations
"""
import subprocess

from passari_workflow.db.models import Base
from alembic.config import Config
from alembic import command

from pathlib import Path


def test_migrate(session, engine, database):
    """
    Test migrations by running all migrations and then downgrading
    """
    config = Config(
        str(Path(__file__).resolve().parent.parent.parent / "alembic.ini")
    )

    try:
        Base.metadata.drop_all(engine)

        # Upgrade the database
        command.upgrade(config, "head")

        # Downgrade the database
        command.downgrade(config, "base")
    finally:
        # Ensure that the tables are the same after the test, even if
        # something fails
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
