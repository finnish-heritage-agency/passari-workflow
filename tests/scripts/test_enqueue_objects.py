import datetime

import pytest
from passari_workflow.queue.queues import QueueType, get_queue
from passari_workflow.scripts.enqueue_objects import \
    cli as enqueue_objects_cli

TEST_DATE = datetime.datetime(
    2019, 1, 2, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
)


@pytest.fixture(scope="function")
def enqueue_objects(cli, monkeypatch, redis):
    def func(args, **kwargs):
        return cli(enqueue_objects_cli, args, **kwargs)

    return func


def test_enqueue_objects(
        redis, session, enqueue_objects, museum_object_factory):
    """
    Enqueue ten objects from a list of 20 objects
    """
    for i in range(0, 20):
        museum_object_factory(
            id=i, preserved=False,
            metadata_hash="", attachment_metadata_hash=""
        )

    result = enqueue_objects(["--object-count", "5"])
    assert "5 object(s) enqueued" in result.stdout

    queue = get_queue(QueueType.DOWNLOAD_OBJECT)
    # Five jobs are enqueued
    assert len(queue.job_ids) == 5

    result = enqueue_objects(["--object-count", "5"])
    assert "5 object(s) enqueued" in result.stdout

    # Five more jobs are enqueued on second run
    assert len(queue.job_ids) == 10

    # Rest of the jobs are enqueued on third run
    result = enqueue_objects(["--object-count", "100"])
    assert "10 object(s) enqueued" in result.stdout

    assert len(queue.job_ids) == 20


def test_enqueue_objects_none_left(
        redis, session, enqueue_objects, museum_object_factory,
        museum_package_factory):
    """
    Try to enqueue objects when none are left to enqueue
    """
    for i in range(0, 20):
        museum_object = museum_object_factory(id=i, preserved=True)
        museum_package = museum_package_factory(
            sip_filename=f"fake_package_{i}.tar",
            downloaded=True,
            packaged=True,
            uploaded=True,
            rejected=True,
            museum_object=museum_object
        )
        museum_object.latest_package = museum_package

    session.commit()

    # No jobs should be enqueued
    result = enqueue_objects(["--object-count", "5"])
    assert "0 object(s) enqueued" in result.stdout


def test_enqueue_objects_with_object_ids(
        redis, session, enqueue_objects, museum_object_factory,
        museum_package_factory):
    """
    Enqueue two specific object IDs
    """
    for i in range(0, 20):
        museum_object_factory(
            id=i, preserved=False,
            metadata_hash="", attachment_metadata_hash=""
        )

    result = enqueue_objects(["--object-ids", "5,8"])
    assert "2 object(s) enqueued" in result.stdout

    queue = get_queue(QueueType.DOWNLOAD_OBJECT)
    assert len(queue.job_ids) == 2

    assert "download_object_5" in queue.job_ids
    assert "download_object_8" in queue.job_ids
