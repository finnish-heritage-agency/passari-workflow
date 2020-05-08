import datetime

import pytest
from passari_workflow.db.models import MuseumObject, MuseumPackage
from passari_workflow.queue.queues import QueueType, get_queue
from passari_workflow.scripts.reenqueue_object import \
    cli as reenqueue_object_cli
from rq import SimpleWorker
from rq.registry import FinishedJobRegistry


def successful_job():
    return ":)"


@pytest.fixture(scope="function")
def reenqueue_object(cli, monkeypatch):
    def func(args, **kwargs):
        return cli(reenqueue_object_cli, args, **kwargs)

    return func


def test_reenqueue_object_success(
        reenqueue_object, session, redis, museum_object, museum_package):
    # Create fake DB entries
    museum_package.downloaded = True
    museum_package.packaged = True
    museum_package.uploaded = True
    museum_package.rejected = True
    session.commit()

    # Create a job that was completed prior to re-enqueuing
    queue = get_queue(QueueType.CONFIRM_SIP)

    queue.enqueue(successful_job, job_id="confirm_sip_123456")
    SimpleWorker([queue], connection=queue.connection).work(burst=True)

    finished_registry = FinishedJobRegistry(queue=queue)
    assert finished_registry.get_job_ids() == ["confirm_sip_123456"]

    result = reenqueue_object(["123456"])

    assert "Object 123456 re-enqueued" in result.stdout

    # New RQ task was enqueued
    queue = get_queue(QueueType.DOWNLOAD_OBJECT)
    assert "download_object_123456" in queue.job_ids

    # Database was updated
    db_museum_object = session.query(MuseumObject).filter_by(id=123456).one()

    assert len(db_museum_object.packages) == 1
    assert not db_museum_object.latest_package

    # Prior finished job was removed
    assert finished_registry.get_job_ids() == []


def test_reenqueue_object_package_not_rejected(
        reenqueue_object, session, redis, museum_object, museum_package):
    # The latest package must have been rejected in order to re-enqueue
    # the object
    museum_package.downloaded = True
    museum_package.packaged = True
    museum_package.uploaded = True
    museum_package.rejected = False
    session.commit()

    with pytest.raises(ValueError) as exc:
        reenqueue_object(["123456"], success=False)

    assert "Latest package fake_package-testID.tar wasn't rejected" \
        in str(exc.value)


def test_reenqueue_object_package_enqueued(
        reenqueue_object, session, redis, museum_object, museum_package):
    # If a task is already enqueued, nothing will be done
    museum_package.downloaded = True
    museum_package.packaged = True
    museum_package.uploaded = True
    museum_package.rejected = True
    session.commit()

    queue = get_queue(QueueType.CREATE_SIP)
    queue.enqueue(
        print, kwargs={"object_id": 123456}, job_id="create_sip_123456"
    )

    with pytest.raises(ValueError) as exc:
        reenqueue_object(["123456"], success=False)

    assert "Object is still in the workflow" in str(exc.value)
