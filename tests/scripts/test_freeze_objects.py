import pytest
from passari_workflow.db.models import (FreezeSource, MuseumObject,
                                        MuseumPackage)
from passari_workflow.exceptions import WorkflowJobRunningError
from passari_workflow.queue.queues import QueueType, get_queue
from passari_workflow.scripts.freeze_objects import cli as freeze_objects_cli
from rq import SimpleWorker
from rq.registry import StartedJobRegistry


@pytest.fixture(scope="function")
def freeze_objects(cli, museum_packages_dir, archive_dir, monkeypatch):
    def func(args, **kwargs):
        return cli(freeze_objects_cli, args, **kwargs)

    monkeypatch.setattr(
        "passari_workflow.scripts.freeze_objects.PACKAGE_DIR",
        str(museum_packages_dir)
    )
    monkeypatch.setattr(
        "passari_workflow.scripts.freeze_objects.ARCHIVE_DIR",
        str(archive_dir)
    )

    return func


def test_freeze_objects(session, freeze_objects, museum_object_factory):
    """
    Freeze two objects and ensure they are no longer in the workflow
    """
    for i in range(0, 5):
        museum_object_factory(id=i, preserved=False)

    # Freeze two objects
    result = freeze_objects([
        "--reason", "Automatic freeze", "--source", "automatic", "0", "1"
    ])
    assert "2 object(s) frozen" in result.stdout
    assert "0 package(s) cancelled" in result.stdout

    object_ids = [
        result[0] for result
        in session.query(MuseumObject.id).filter_by(
            freeze_reason="Automatic freeze",
            freeze_source=FreezeSource.AUTOMATIC
        ).all()
    ]

    assert set([0, 1]) == set(object_ids)


def test_freeze_objects_default_source(
        session, freeze_objects, museum_object_factory):
    """
    Freeze object without explicitly defining a source, and check that it
    defaults to FreezeSource.USER
    """
    museum_object_factory(id=3, preserved=False)

    freeze_objects([
        "--reason", "Object 3: freeze", "3"
    ])

    # Source defaults to FreezeSource.USER
    assert session.query(MuseumObject).filter_by(
        id=3, freeze_source=FreezeSource.USER,
        freeze_reason="Object 3: freeze",
        frozen=True
    ).count() == 1


def test_freeze_objects_delete_jobs(
        session, redis, freeze_objects, museum_object_factory):
    """
    Freeze object with one pending and one failed job, and ensure
    they are both deleted
    """
    def successful_job():
        return ":)"

    def failing_job():
        raise RuntimeError(":(")

    museum_object_factory(id=123456)

    queue_a = get_queue(QueueType.DOWNLOAD_OBJECT)
    queue_b = get_queue(QueueType.SUBMIT_SIP)

    queue_a.enqueue(successful_job, job_id="download_object_123456")
    queue_b.enqueue(failing_job, job_id="submit_sip_123456")
    SimpleWorker([queue_b], connection=queue_b.connection).work(burst=True)

    freeze_objects([
        "--delete-jobs", "--reason", "Deleting job", "123456"
    ])

    assert len(queue_a.job_ids) == 0
    assert len(queue_b.job_ids) == 0

    assert session.query(MuseumObject).filter_by(
        id=123456, freeze_reason="Deleting job"
    ).count() == 1


def test_freeze_objects_running_jobs(
        session, redis, freeze_objects, museum_object_factory):
    """
    Try freezing two objects when they have running jobs.
    """
    def successful_job():
        return ":)"

    museum_object_factory(id=123456)
    museum_object_factory(id=654321)

    queue = get_queue(QueueType.DOWNLOAD_OBJECT)
    started_registry = StartedJobRegistry(queue=queue)
    job_a = queue.enqueue(successful_job, job_id="download_object_123456")
    job_b = queue.enqueue(successful_job, job_id="download_object_654321")
    started_registry.add(job_a, -1)
    started_registry.add(job_b, -1)

    with pytest.raises(WorkflowJobRunningError) as exc:
        freeze_objects([
            "--reason", "Won't succeed", "654321", "123456"
        ], success=False)

    assert "can't be frozen: 123456, 654321" in str(exc.value)


def test_freeze_objects_existing_dirs(
        session, redis, museum_packages_dir, freeze_objects,
        museum_object_factory):
    """
    Try freezing two objects when they have existing directories
    """
    museum_object_factory(id=123456)
    museum_object_factory(id=654321)

    (museum_packages_dir / "123456" / "sip" / "reports").mkdir(parents=True)
    (museum_packages_dir / "654321" / "sip" / "reports").mkdir(parents=True)

    freeze_objects([
        "--delete-jobs", "--reason", "Test reason", "654321", "123456"
    ])

    # Directories were removed when the corresponding objects were frozen
    assert not (museum_packages_dir / "123456").is_dir()
    assert not (museum_packages_dir / "654321").is_dir()


def test_freeze_objects_cancel_package(
        session, freeze_objects, redis, museum_object_factory,
        museum_package_factory):
    """
    Test that freezing an object causes the corresponding package to be
    cancelled if it hasn't been processed in the DPRES service yet
    """
    museum_object_a = museum_object_factory(id=10)
    museum_object_b = museum_object_factory(id=20)

    museum_package_a = museum_package_factory(
        id=100, sip_filename="testA.tar", museum_object=museum_object_a
    )
    museum_package_b = museum_package_factory(
        id=200, sip_filename="testB.tar", museum_object=museum_object_b,
        rejected=True
    )

    museum_object_a.latest_package = museum_package_a
    museum_object_b.latest_package = museum_package_b

    session.commit()

    # Freeze two objects
    result = freeze_objects(["--reason", "Failure", "10", "20"])
    assert "2 object(s) frozen" in result.stdout
    assert "1 package(s) cancelled" in result.stdout

    museum_package_a = session.query(MuseumPackage).get(100)
    museum_package_b = session.query(MuseumPackage).get(200)


def test_freeze_objects_archive_log_files(
        session, freeze_objects, redis, museum_object_factory,
        local_museum_package_factory, museum_packages_dir,
        archive_dir):
    """
    Test that freezing an object causes the log files to be archived
    if the object directory with logs still exists
    """
    museum_object = museum_object_factory(id=10)
    museum_package = local_museum_package_factory(
        id=100, sip_filename="testA.tar", museum_object=museum_object
    )
    museum_object.latest_package = museum_package

    session.commit()

    (museum_packages_dir / "10" / "logs" / "import-object.log").write_text(
        "Test log"
    )

    # Freeze one object
    result = freeze_objects(["--reason", "Test reason", "10"])
    assert "1 object(s) frozen" in result.stdout

    # Logs were archived
    assert (
        archive_dir / "10" / "Object_10" / "20181116_Object_10.tar" / "logs"
        / "import-object.log.gz"
    ).is_file()
