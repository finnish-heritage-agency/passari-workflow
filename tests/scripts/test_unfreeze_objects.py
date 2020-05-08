import pytest
from passari_workflow.db.models import MuseumObject
from passari_workflow.queue.queues import QueueType, get_queue
from passari_workflow.scripts.unfreeze_objects import \
    cli as unfreeze_objects_cli


@pytest.fixture(scope="function")
def unfreeze_objects(cli):
    def func(args, **kwargs):
        return cli(unfreeze_objects_cli, args, **kwargs)

    return func


def test_unfreeze_objects(session, unfreeze_objects, museum_object_factory):
    museum_object_factory(id=10, frozen=True, freeze_reason="Test reason")
    museum_object_factory(id=20, frozen=True, freeze_reason="Test reason")
    museum_object_factory(id=30, frozen=True, freeze_reason="Test reason")

    # Unfreeze the first object
    result = unfreeze_objects([
        "--with-object-ids", "10", "--with-reason", "Test reason"
    ])

    assert "1 object(s) were updated" in result.stdout
    assert session.query(
        MuseumObject
    ).filter_by(frozen=False, id=10).count() == 1

    # Unfreeze the second and third object
    result = unfreeze_objects(["--with-reason", "Test reason"])

    assert "2 object(s) were updated" in result.stdout
    assert (
        session.query(MuseumObject)
        .filter_by(frozen=False)
        .filter(MuseumObject.id.in_([20, 30]))
        .count() == 2
    )

    queue = get_queue(QueueType.DOWNLOAD_OBJECT)

    # Museum object is not enqueued by default
    assert len(queue.job_ids) == 0


def test_unfreeze_objects_enqueue(
        session, unfreeze_objects, museum_object_factory):
    """
    Test that an object is enqueued after unfreezing if the command-line
    flag is used
    """
    museum_object_factory(id=10, frozen=True, freeze_reason="Test reason")

    result = unfreeze_objects(["--with-reason", "Test reason", "--enqueue"])

    assert "1 object(s) were updated" in result.stdout
    assert session.query(
        MuseumObject
    ).filter_by(frozen=False, id=10).count() == 1

    queue = get_queue(QueueType.DOWNLOAD_OBJECT)

    # Job was enqueued
    assert "download_object_10" in queue.job_ids


def test_unfreeze_objects_remove_latest_package(
        session, unfreeze_objects, museum_object_factory,
        museum_package_factory):
    """
    Test unfreezing an object with a latest package, and ensure the link
    to the latest package is removed unless the package was preserved
    """
    museum_object_a = museum_object_factory(
        id=10, frozen=True, freeze_reason="Test reason 1"
    )
    museum_object_b = museum_object_factory(
        id=20, frozen=True, freeze_reason="Test reason 2"
    )

    museum_package_a = museum_package_factory(
        id=100, sip_filename="testA.tar", preserved=True,
        museum_object=museum_object_a
    )
    museum_package_b = museum_package_factory(
        id=200, sip_filename="testB.tar", rejected=True,
        museum_object=museum_object_b
    )

    museum_object_a.latest_package = museum_package_a
    museum_object_b.latest_package = museum_package_b

    session.commit()

    # Unfreeze the first object; the latest package should *not* be unlinked
    result = unfreeze_objects(["--with-reason", "Test reason 1"])

    assert "1 object(s) were updated" in result.stdout

    museum_object_a = session.query(MuseumObject).get(10)
    assert not museum_object_a.frozen
    assert museum_object_a.latest_package

    # Unfreeze the first object; the latest package should be unlinked
    # because it was not a preserved one
    result = unfreeze_objects(["--with-reason", "Test reason 2"])

    assert "1 object(s) were updated" in result.stdout

    museum_object_b = session.query(MuseumObject).get(20)
    assert not museum_object_b.frozen
    assert not museum_object_b.latest_package


def test_unfreeze_objects_no_parameters(session, unfreeze_objects):
    with pytest.raises(ValueError) as exc:
        unfreeze_objects([])

    assert "Either 'reason' or 'object_ids'" in str(exc.value)
