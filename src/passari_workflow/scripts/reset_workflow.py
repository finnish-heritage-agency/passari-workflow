"""
Reset a workflow after restoring a PostgreSQL backup to restore the workflow
into a consistent state and allow the dangling objects to be preserved
"""
import shutil
from pathlib import Path

import click
from sqlalchemy.sql import or_

from passari_workflow.config import PACKAGE_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumObject, MuseumPackage
from passari_workflow.queue.queues import lock_queues


def reset_workflow():
    """
    Reset workflow after a PostgreSQL backup restoration by removing in-process
    packages that were not submitted to the DPRES service but were still
    in the workflow at the time the backup was initiated.
    """
    with lock_queues():
        connect_db()

        with scoped_session() as db:
            # Get objects that have been downloaded or packaged, but which
            # haven't been uploaded yet
            objects = (
                db.query(MuseumObject)
                .join(
                    MuseumPackage,
                    MuseumPackage.id == MuseumObject.latest_package_id
                )
                .filter(
                    MuseumPackage.uploaded == False,
                    or_(
                        MuseumPackage.downloaded,
                        MuseumPackage.packaged
                    )
                )
            )
            objects = list(objects)

            print(f"Found {len(objects)} dangling objects")

            for mus_object in objects:
                mus_package = mus_object.latest_package

                # Remove the lingering package from the MuseumObject to make
                # the object eligible for preservation again.
                mus_object.latest_package = None
                db.delete(mus_package)

                try:
                    shutil.rmtree(Path(PACKAGE_DIR) / str(mus_object.id))
                except OSError:
                    # Directory does not exist; ignore
                    pass

    print("Done!")


@click.command(help=(
    "Reset workflow following a PostgreSQL backup restoration by removing "
    "dangling packages in the database.\n\n"
    "This is required after restoring from a backup to restore the workflow "
    "into a consistent state, since the corresponding workflow jobs do not "
    "exist anymore.\n\n"
    "BEFORE PERFORMING A RESET ensure no jobs are on the workflow."
))
@click.option(
    "--perform-reset", is_flag=True, default=False,
    help="Reset the workflow"
)
def cli(perform_reset):
    if perform_reset:
        reset_workflow()
    else:
        # Print the help text if the flag isn't given
        click.echo(
            cli.get_help(click.get_current_context())
        )


if __name__ == "__main__":
    cli()
