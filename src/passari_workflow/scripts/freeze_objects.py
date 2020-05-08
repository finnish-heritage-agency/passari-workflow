"""
Freeze objects to stop preservation
"""
import shutil
from pathlib import Path

import click

from passari.dpres.package import MuseumObjectPackage
from passari_workflow.config import ARCHIVE_DIR, PACKAGE_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import (FreezeSource, MuseumObject,
                                               MuseumPackage)
from passari_workflow.exceptions import WorkflowJobRunningError
from passari_workflow.queue.queues import (delete_jobs_for_object_id,
                                                  get_running_object_ids,
                                                  lock_queues)


def freeze_objects(object_ids, reason, source, delete_jobs=True):
    """
    Freeze objects to prevent them from being included in the preservation
    workflow

    :returns: (freeze_count, cancel_count) tuple for how many objects were
              frozen and how many packages were cancelled as a result
    """
    object_ids = [int(object_id) for object_id in object_ids]
    source = FreezeSource(source)

    with lock_queues():
        # Are there object IDs that we're about to freeze but that are
        # still running?
        running_object_ids = get_running_object_ids()
        conflicting_object_ids = set(object_ids) & set(running_object_ids)

        if conflicting_object_ids:
            raise WorkflowJobRunningError(
                "The following object IDs have running jobs and can't be "
                f"frozen: {', '.join([str(o) for o in sorted(conflicting_object_ids)])}"
            )

        connect_db()
        with scoped_session() as db:
            freeze_count = (
                db.query(MuseumObject)
                .filter(MuseumObject.id.in_(object_ids))
                .update({
                    MuseumObject.frozen: True,
                    MuseumObject.freeze_reason: reason,
                    MuseumObject.freeze_source: source
                }, synchronize_session=False)
            )

            packages_to_cancel = list(
                db.query(MuseumPackage)
                .join(
                    MuseumObject,
                    MuseumObject.latest_package_id == MuseumPackage.id
                )
                .filter(
                    MuseumPackage.museum_object_id.in_(object_ids),
                    MuseumPackage.preserved == False,
                    MuseumPackage.rejected == False,
                    MuseumPackage.cancelled == False
                )
            )

            for package in packages_to_cancel:
                package.cancelled = True

                try:
                    museum_package = MuseumObjectPackage.from_path_sync(
                        Path(PACKAGE_DIR) / str(package.museum_object_id),
                        sip_id=package.sip_id
                    )
                    museum_package.copy_log_files_to_archive(ARCHIVE_DIR)
                except FileNotFoundError:
                    # If the SIP doesn't exist, just skip it
                    pass

            # Cancel any jobs for each object ID if enabled
            if delete_jobs:
                for object_id in object_ids:
                    delete_jobs_for_object_id(object_id)

                    # Delete the museum package directory
                    try:
                        shutil.rmtree(Path(PACKAGE_DIR) / str(object_id))
                    except OSError:
                        # Directory does not exist
                        pass

        return freeze_count, len(packages_to_cancel)


@click.command()
@click.option(
    "--source",
    help=(
        "Source of the freeze (eg. whether freezing the object was done "
        "manually by the user or the workflow)"
    ),
    type=click.Choice([field.name.lower() for field in FreezeSource]),
    default="user"
)
@click.option("--reason", type=str, help="Reason for freezing the object(s)")
@click.option(
    "--delete-jobs/--no-delete-jobs", default=False,
    help=(
        "Delete workflow jobs and files for the frozen objects. "
        "Default is false."
    )
)
@click.argument("object_id", type=int, nargs=-1)
def cli(source, reason, delete_jobs, object_id):
    freeze_count, cancel_count = freeze_objects(
        object_ids=object_id,
        reason=reason,
        source=source,
        delete_jobs=delete_jobs
    )
    print(
        f"{freeze_count} object(s) frozen, "
        f"{cancel_count} package(s) cancelled"
    )


if __name__ == "__main__":
    cli()
