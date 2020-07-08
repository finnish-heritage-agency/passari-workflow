"""
Enqueue objects to be downloaded in the background.

Similar to 'enqueue-objects', but objects are enqueued using a background RQ
job, ensuring the command returns immediately
"""
import click

from passari_workflow.queue.queues import QueueType, get_queue
from passari_workflow.jobs.enqueue_objects import enqueue_objects


def deferred_enqueue_objects(object_count):
    """
    Enqueue given number of objects to the preservation workflow using a
    background RQ job

    :param int object_count: How many objects to enqueue at most
    """
    queue = get_queue(QueueType.ENQUEUE_OBJECTS)
    queue.enqueue(enqueue_objects, kwargs={"object_count": object_count})

    print(f"{object_count} object(s) will be enqueued")

    return object_count


@click.command()
@click.option(
    "--object-count", default=10, help="How many objects to enqueue")
def cli(object_count):
    deferred_enqueue_objects(object_count)


if __name__ == "__name__":
    cli()
