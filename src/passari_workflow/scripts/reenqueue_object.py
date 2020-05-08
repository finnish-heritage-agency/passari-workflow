"""
Re-enqueue a failed object after rejection by DPRES service
"""
import click

from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumObject, MuseumPackage
from passari_workflow.jobs.download_object import download_object
from passari_workflow.queue.queues import (QueueType,
                                                  get_enqueued_object_ids,
                                                  get_queue,
                                                  delete_jobs_for_object_id)


def reenqueue_object(object_id: int):
    """
    Re-enqueue rejected object into the workflow
    """
    object_id = int(object_id)
    connect_db()

    queue = get_queue(QueueType.DOWNLOAD_OBJECT)

    with scoped_session() as db:
        museum_object = (
            db.query(MuseumObject)
            .join(
                MuseumPackage,
                MuseumObject.latest_package_id == MuseumPackage.id
            )
            .filter(MuseumObject.id == object_id)
            .one()
        )

        if museum_object.latest_package and \
                not museum_object.latest_package.rejected:
            raise ValueError(
                f"Latest package {museum_object.latest_package.sip_filename} "
                f"wasn't rejected"
            )

        object_ids = get_enqueued_object_ids()

        if object_id in object_ids:
            raise ValueError(
                f"Object is still in the workflow and can't be re-enqueued"
            )

        museum_object.latest_package = None

        delete_jobs_for_object_id(object_id)

        queue.enqueue(
            download_object, kwargs={"object_id": object_id},
            job_id=f"download_object_{object_id}"
        )


@click.command()
@click.argument("object_id", nargs=1)
def cli(object_id):
    reenqueue_object(object_id)

    print(f"Object {object_id} re-enqueued")


if __name__ == "__main__":
    cli()
