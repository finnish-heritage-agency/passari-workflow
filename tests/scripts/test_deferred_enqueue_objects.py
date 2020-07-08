import pytest
from passari_workflow.queue.queues import QueueType, get_queue
from passari_workflow.scripts.deferred_enqueue_objects import \
    cli as deferred_enqueue_objects_cli
from rq import SimpleWorker


@pytest.fixture(scope="function")
def deferred_enqueue_objects(cli, redis):
    def func(args, **kwargs):
        return cli(deferred_enqueue_objects_cli, args, **kwargs)

    return func


def test_deferred_enqueue_objects(
        redis, session, deferred_enqueue_objects, museum_object_factory):
    """
    Enqueue 5 objects from a list of 20 objects
    """
    for i in range(0, 20):
        museum_object_factory(
            id=i, preserved=False,
            metadata_hash="", attachment_metadata_hash=""
        )

    result = deferred_enqueue_objects(["--object-count", "5"])
    assert "5 object(s) will be enqueued" in result.stdout

    queue = get_queue(QueueType.ENQUEUE_OBJECTS)
    # One job is enqueued
    assert len(queue.job_ids) == 1

    # Finish the job
    SimpleWorker([queue], connection=queue.connection).work(burst=True)

    # 5 objects should now be enqueued
    queue = get_queue(QueueType.DOWNLOAD_OBJECT)
    assert len(queue.job_ids) == 5
