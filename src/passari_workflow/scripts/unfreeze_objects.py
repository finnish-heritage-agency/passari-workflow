"""
Unfreeze objects to allow them to be preserved
"""
import click

from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumObject, MuseumPackage
from passari_workflow.queue.queues import lock_queues
from passari_workflow.scripts.enqueue_objects import enqueue_object


def unfreeze_objects(reason=None, object_ids=None, enqueue=False):
    """
    Unfreeze objects with the given reason and/or object IDs.

    This allows them to be preserved again.

    :param str reason: Unfreeze objects with this reason
    :param list object_ids: Objects to unfreeze.
    :param bool enqueue: Whether to enqueue the unfrozen objects immediately.
                         Default is False.
    """
    connect_db()

    if not reason and not object_ids:
        raise ValueError("Either 'reason' or 'object_ids' has to be provided")

    with lock_queues():
        with scoped_session() as db:
            query = (
                db.query(MuseumObject)
                .outerjoin(
                    MuseumPackage,
                    MuseumPackage.id == MuseumObject.latest_package_id
                )
                .filter(MuseumObject.frozen == True)
            )

            if reason:
                query = query.filter(MuseumObject.freeze_reason == reason)
            if object_ids:
                object_ids = [int(object_id) for object_id in object_ids]
                query = query.filter(MuseumObject.id.in_(object_ids))

            museum_objects = list(query)
            for museum_object in museum_objects:
                museum_object.frozen = False
                museum_object.freeze_reason = None
                museum_object.freeze_source = None

                # Remove the latest package if it was *not* successfully
                # preserved to ensure the object is eligible for preservation
                remove_latest_package = (
                    museum_object.latest_package
                    and not museum_object.latest_package.preserved
                )

                if remove_latest_package:
                    museum_object.latest_package = None

                if enqueue:
                    enqueue_object(object_id=museum_object.id)

            return len(museum_objects)


@click.command()
@click.option(
    "--with-reason", help="Unfreeze objects with this reason", type=str
)
@click.option(
    "--with-object-ids",
    help=(
        "Unfreeze object(s) with these object IDs. Accepts a comma-separated "
        "list of object IDs."
    ),
    type=str, default=""
)
@click.option(
    "--enqueue/--no-enqueue",
    help=(
        "Whether to enqueue the object(s) after unfreezing. Default is false."
    ),
    default=False
)
def cli(with_reason, with_object_ids, enqueue):
    if with_object_ids == "":
        with_object_ids = []
    else:
        with_object_ids = with_object_ids.split(",")

    update_count = unfreeze_objects(
        reason=with_reason,
        object_ids=with_object_ids,
        enqueue=enqueue
    )

    print(f"{update_count} object(s) were updated")


if __name__ == "__main__":
    cli()
