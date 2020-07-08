"""
Enqueue objects to be downloaded
"""
import click
from sqlalchemy.sql.expression import func

from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumObject
from passari_workflow.jobs.download_object import download_object
from passari_workflow.queue.queues import (QueueType, get_enqueued_object_ids,
                                           get_queue, lock_queues)


def enqueue_object(object_id):
    """
    Enqueue a single object.

    This can be called separately outside of 'enqueue_objects'. In this case,
    the caller needs to ensure the workflow is locked.
    """
    object_id = int(object_id)
    queue = get_queue(QueueType.DOWNLOAD_OBJECT)

    job_id = f"download_object_{object_id}"
    return queue.enqueue(
        download_object,
        kwargs={"object_id": object_id},
        job_id=job_id
    )


def enqueue_objects(object_count, random=False, object_ids=None):
    """
    Enqueue given number of objects to the preservation workflow.

    :param int object_count: How many objects to enqueue at most
    :param bool random: Whether to enqueue objects at random instead
                        of in-order.
    :param list object_ids: Object IDs to enqueue. If provided, 'object_count'
                            and 'random' are ignored.
    """
    if object_ids:
        object_count = len(object_ids)

    with lock_queues():
        connect_db()
        enqueued_object_ids = get_enqueued_object_ids()

        new_job_count = 0

        with scoped_session() as db:
            object_query = (
                db.query(MuseumObject)
                .with_transformation(MuseumObject.filter_preservation_pending)
                .yield_per(500)
            )

            if object_ids:
                object_query = object_query.filter(
                    MuseumObject.id.in_(object_ids)
                )

            if random:
                object_query = object_query.order_by(func.random())

            for museum_object in object_query:
                if museum_object.id not in enqueued_object_ids:
                    enqueue_object(museum_object.id)
                    new_job_count += 1
                    print(f"Enqueued download_object_{museum_object.id}")

                if new_job_count >= object_count:
                    break

    print(f"{new_job_count} object(s) enqueued for download")

    return new_job_count


@click.command()
@click.option(
    "--object-count", default=10, help="How many objects to enqueue")
@click.option(
    "--random/--no-random", default=False,
    help=(
        "Enqueue objects in random order. Should be only used for "
        "pre-production tests."
    )
)
@click.option(
    "--object-ids", default=None, type=str,
    help=(
        "Comma-separated list of object IDs. If provided, only the provided "
        "objects will be enqueued."
    )
)
def cli(object_count, random, object_ids):
    if object_ids:
        object_ids = [int(object_id) for object_id in object_ids.split(",")]

    enqueue_objects(
        object_count=object_count, random=random, object_ids=object_ids
    )


if __name__ == "__main__":
    cli()
