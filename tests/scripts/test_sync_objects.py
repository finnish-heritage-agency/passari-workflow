import datetime
import hashlib

import pytest
from passari_workflow.db.models import (MuseumAttachment, MuseumObject,
                                               SyncStatus)
from passari_workflow.scripts.sync_objects import \
    cli as sync_objects_cli


MOCK_OBJECTS = []

for i in range(1, 11):
    date = datetime.datetime(
        2018, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
    )
    # If the ID is odd, include two attachments
    if i % 2 != 0:
        multimedia_ids = [i+10, i+11]
    else:
        multimedia_ids = []

    # The last object has no creation or modification date
    if i == 10:
        modified_date = None
        created_date = None
    else:
        modified_date = date + datetime.timedelta(days=i)
        created_date = date

    MOCK_OBJECTS.append({
        "id": i,
        "title": f"Object {i}",
        "modified_date": modified_date,
        "created_date": created_date,
        "multimedia_ids": multimedia_ids,
        "xml_hash": hashlib.sha256(f"Object {i}".encode("utf-8")).hexdigest()
    })


@pytest.fixture(scope="function", autouse=True)
def mock_iterate_objects(monkeypatch):
    async def mock_iterate_objects(session, offset=0, modify_date_gte=None):
        for result in MOCK_OBJECTS[offset:]:
            if modify_date_gte is not None:
                # If modification date is provided for filtering, use it
                if result["modified_date"] and \
                        result["modified_date"] < modify_date_gte:
                    continue

            yield result

    monkeypatch.setattr(
        "passari_workflow.scripts.sync_objects.iterate_objects",
        mock_iterate_objects
    )


@pytest.fixture(scope="function")
def mock_iterate_objects_crash(monkeypatch):
    """
    Mock 'iterate_objects', but with keyboard interrupts that occur every
    3 objects.

    This is used to test the '--save-progress' feature
    """
    async def mock_iterate_objects(session, offset=0, modify_date_gte=None):
        count = 0
        for result in MOCK_OBJECTS[offset:]:
            # If modification date is provided for filtering, use it
            if modify_date_gte is not None:
                if result["modified_date"] and \
                        result["modified_date"] < modify_date_gte:
                    continue

            yield result
            count += 1

            if count == 3:
                raise KeyboardInterrupt()

    monkeypatch.setattr(
        "passari_workflow.scripts.sync_objects.iterate_objects",
        mock_iterate_objects
    )
    # Use lower chunk size to ensure progress is saved once 3 objects are
    # processed
    monkeypatch.setattr(
        "passari_workflow.scripts.sync_objects.CHUNK_SIZE", 3
    )


@pytest.fixture(scope="function")
def sync_objects(cli):
    def func(args, **kwargs):
        return cli(sync_objects_cli, args, **kwargs)

    return func


def test_sync_objects(sync_objects, session):
    """
    Sync 10 objects and ensure objects are created on first run and updated
    on the second run
    """
    result = sync_objects([])
    assert "10 inserts" in result.stdout
    assert "0 updates" in result.stdout

    assert session.query(MuseumObject).count() == 10
    db_museum_object = session.query(MuseumObject).get(5)
    assert db_museum_object.title == "Object 5"
    assert db_museum_object.metadata_hash == \
        "02e1774cdc3b90500e60cc6bb0274dfc6172775116a848eee42de8a34b2d1797"
    assert db_museum_object.created_date.timestamp() == datetime.datetime(
        2018, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()
    assert db_museum_object.modified_date.timestamp() == datetime.datetime(
        2018, 1, 6, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()

    # Check that two attachments were created
    assert len(db_museum_object.attachments) == 2
    assert [
        attachment.id for attachment in db_museum_object.attachments
    ] == [15, 16]

    # Modify the museum object's modification date, and check that it's
    # updated back
    db_museum_object.modified_date = datetime.datetime(
        2010, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
    )

    # Modify attachment's filename and check that it is NOT modified
    # after another sync
    db_attachment = session.query(MuseumAttachment).get(15)
    db_attachment.filename = "testFilename.tar"

    session.commit()

    result = sync_objects([])
    assert "0 inserts" in result.stdout
    assert "10 updates" in result.stdout

    db_museum_object = session.query(MuseumObject).get(5)
    assert db_museum_object.modified_date.timestamp() == datetime.datetime(
        2018, 1, 6, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()

    db_museum_attachment = session.query(MuseumAttachment).get(15)
    assert db_museum_attachment.filename == "testFilename.tar"


def test_sync_objects_newer_modification_date(sync_objects, session):
    """
    Sync an object with a newer modification date and ensure it's
    not updated
    """
    sync_objects([])

    # Give the first object a newer modification date
    # This corresponds to an object that has had one of its attachments
    # modified, and won't be updated as it's newer than the object's
    # modification date
    db_museum_object_a = session.query(MuseumObject).get(5)
    db_museum_object_a.modified_date = datetime.datetime(
        2019, 6, 1, 12, 0, tzinfo=datetime.timezone.utc
    )

    # Give the second object an older modification date
    # This will be updated back.
    db_museum_object_b = session.query(MuseumObject).get(6)
    db_museum_object_b.modified_date = datetime.datetime(
        2011, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
    )
    session.commit()

    sync_objects([])

    db_museum_object_a = session.query(MuseumObject).get(5)
    db_museum_object_b = session.query(MuseumObject).get(6)

    assert db_museum_object_a.modified_date.timestamp() == datetime.datetime(
        2019, 6, 1, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()
    assert db_museum_object_b.modified_date.timestamp() == datetime.datetime(
        2018, 1, 7, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()


def test_sync_objects_offset(sync_objects, session):
    """
    Sync only 4 objects by using an offset
    """
    result = sync_objects(["--offset", "6"])

    assert "4 inserts" in result.stdout
    assert session.query(MuseumObject).count() == 4


def test_sync_objects_limit(sync_objects, session):
    result = sync_objects(["--limit", "3"])

    assert "3 inserts" in result.stdout
    assert session.query(MuseumObject).count() == 3


def test_sync_objects_default_date(sync_objects, session):
    """
    Check that a default date is used for objects that don't have
    creation or modification dates
    """
    sync_objects([])

    # Last object in test dataset has no dates
    db_museum_object = session.query(MuseumObject).get(10)

    assert not db_museum_object.created_date
    assert not db_museum_object.modified_date


@pytest.mark.usefixtures("mock_iterate_objects_crash")
def test_sync_objects_save_progress(sync_objects, session, freeze_time):
    """
    Use --save-progress flag to save progress. Ensure progress is saved
    and used correctly
    """
    freeze_time("2019-02-01")
    sync_objects(["--save-progress"], success=False)

    # 3 objects were processed before the script stopped
    sync_status = (
        session.query(SyncStatus)
        .filter_by(name="sync_objects")
        .first()
    )
    assert session.query(MuseumObject).count() == 3
    assert sync_status.offset == 3
    assert sync_status.start_sync_date == datetime.datetime(
        2019, 2, 1, tzinfo=datetime.timezone.utc
    )

    # Change time; this shouldn't affect anything since we only fetch the date
    # at the start of synchronization run.
    freeze_time("2019-02-02")

    # Run the script three more times to ensure all objects are synced
    for _ in range(0, 2):
        sync_objects(["--save-progress"], success=False)

    sync_objects(["--save-progress"])

    # Sync status is reset, but the last finished synchronization date remains
    sync_status = (
        session.query(SyncStatus)
        .filter_by(name="sync_objects")
        .first()
    )
    assert sync_status.offset == 0
    assert sync_status.prev_start_sync_date == datetime.datetime(
        2019, 2, 1, tzinfo=datetime.timezone.utc
    )
    assert not sync_status.start_sync_date

    assert session.query(MuseumObject).count() == 10


def test_sync_objects_second_run_filter(
        sync_objects, session, sync_status_factory, freeze_time):
    """
    Start a second synchronization run and ensure only a subset of objects
    are synchronized
    """
    sync_status_factory(
        name="sync_objects",
        prev_start_sync_date=datetime.datetime(
            2018, 1, 7, 12, 0, tzinfo=datetime.timezone.utc
        )
    )

    freeze_time("2019-02-02")

    # Only 5 objects will be synchronized. Out of the 5, 4 have an older
    # modification date and one has no modification date at all
    result = sync_objects(["--save-progress"])

    assert "5 inserts" in result.stdout
    assert session.query(MuseumObject).count() == 5

    sync_status = (
        session.query(SyncStatus)
        .filter_by(name="sync_objects")
        .first()
    )

    assert sync_status.offset == 0
    assert not sync_status.start_sync_date

    # Next synchronization run will iterate starting from this date
    assert sync_status.prev_start_sync_date == datetime.datetime(
        2019, 2, 2, tzinfo=datetime.timezone.utc
    )
