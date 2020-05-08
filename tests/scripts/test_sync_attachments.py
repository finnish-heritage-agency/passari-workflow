import datetime
import hashlib
from pathlib import Path

import pytest
from passari_workflow.db.models import (MuseumAttachment, MuseumObject,
                                               SyncStatus)
from passari_workflow.scripts.sync_attachments import \
    cli as sync_attachments_cli

MOCK_MULTIMEDIA = []

for i in range(11, 21):
    date = datetime.datetime(
        2018, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
    )

    # If the ID is odd, include two objects
    if i % 2 != 0:
        object_ids = [i-10, i-9]
    else:
        object_ids = []

    if i == 20:
        modified_date = None
        created_date = None
    else:
        modified_date = date + datetime.timedelta(days=i)
        created_date = date

    MOCK_MULTIMEDIA.append({
        "id": i,
        "filename": f"test_{i}.jpg",
        "modified_date": modified_date,
        "created_date": created_date,
        "object_ids": object_ids,
        "xml_hash": hashlib.sha256(f"Object {i}".encode("utf-8")).hexdigest()
    })


@pytest.fixture(scope="function", autouse=True)
def mock_sync_attachments(monkeypatch):
    async def mock_iterate_multimedia(session, offset=0, modify_date_gte=None):
        for result in MOCK_MULTIMEDIA[offset:]:
            # If modification date is provided for filtering, use it
            if modify_date_gte is not None:
                if result["modified_date"] and \
                        result["modified_date"] < modify_date_gte:
                    continue

            yield result

    monkeypatch.setattr(
        "passari_workflow.scripts.sync_attachments.iterate_multimedia",
        mock_iterate_multimedia
    )


@pytest.fixture(scope="function")
def sync_attachments(cli):
    def func(args, **kwargs):
        return cli(sync_attachments_cli, args, **kwargs)

    return func


@pytest.fixture(scope="function")
def mock_iterate_attachments_crash(monkeypatch):
    """
    Mock 'iterate_attachments', but with keyboard interrupts that occur every
    3 objects.

    This is used to test the '--save-progress' feature
    """
    async def mock_iterate_attachments(
            session, offset=0, modify_date_gte=None):
        count = 0
        for result in MOCK_MULTIMEDIA[offset:]:
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
        "passari_workflow.scripts.sync_attachments.iterate_multimedia",
        mock_iterate_attachments
    )
    # Use lower chunk size to ensure progress is saved once 3 attachments are
    # processed
    monkeypatch.setattr(
        "passari_workflow.scripts.sync_attachments.CHUNK_SIZE", 3
    )


def test_sync_attachments(sync_attachments, session):
    """
    Sync 10 attachments and ensure attachments are created on first run
    and updated on the second run
    """
    result = sync_attachments([])
    assert "10 inserts" in result.stdout
    assert "0 updates" in result.stdout

    assert session.query(MuseumAttachment).count() == 10
    db_attachment = session.query(MuseumAttachment).get(15)
    assert db_attachment.filename == "test_15.jpg"
    assert db_attachment.metadata_hash == \
        "6466d255fe431b3bc229ccb477738b6e84eb7aad82ee070e93c07e37691aee38"
    assert db_attachment.created_date.timestamp() == datetime.datetime(
        2018, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()
    assert db_attachment.modified_date.timestamp() == datetime.datetime(
        2018, 1, 16, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()

    # Check that two attachments were created
    assert len(db_attachment.museum_objects) == 2
    assert [
        mus_object.id for mus_object in db_attachment.museum_objects
    ] == [5, 6]

    # Modify the attachment's modification date, and check that it's updated
    # back
    db_attachment.modified_date = datetime.datetime(
        2010, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
    )

    # Modify the object's title and check that it is NOT modified after
    # another sync
    db_mus_object = session.query(MuseumObject).get(5)
    db_mus_object.title = "Fake title"

    session.commit()

    result = sync_attachments([])
    assert "0 inserts" in result.stdout
    assert "10 updates" in result.stdout

    db_attachment = session.query(MuseumAttachment).get(15)
    assert db_attachment.modified_date.timestamp() == datetime.datetime(
        2018, 1, 16, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()

    db_mus_object = session.query(MuseumObject).get(5)
    assert db_mus_object.title == "Fake title"


def test_sync_attachments_object_modified_date(
        sync_attachments, museum_object_factory, session):
    """
    Sync an attachment with a newer modification date than the related
    object. The object should inherit the modification date from
    the attachment since it's more recent.
    """
    museum_object_factory(
        id=5,
        modified_date=datetime.datetime(
            2011, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
        )
    )

    sync_attachments([])

    db_mus_object = session.query(MuseumObject).get(5)

    assert db_mus_object.modified_date.timestamp() == datetime.datetime(
        2018, 1, 16, 12, 0, tzinfo=datetime.timezone.utc
    ).timestamp()
    assert db_mus_object.modified_date == \
        db_mus_object.attachments[0].modified_date


def test_sync_attachments_offset(sync_attachments, session):
    """
    Sync only 4 attachments by using an offset
    """
    result = sync_attachments(["--offset", "6"])

    assert "4 inserts" in result.stdout
    assert session.query(MuseumAttachment).count() == 4


def test_sync_attachments_limit(sync_attachments, session):
    """
    Sync only 3 attachments by using a limit
    """
    result = sync_attachments(["--limit", "3"])

    assert "3 inserts" in result.stdout
    assert session.query(MuseumAttachment).count() == 3


def test_sync_attachments_default_date(sync_attachments, session):
    """
    Check that default date is used for attachments that don't have
    creation or modification dates
    """
    sync_attachments([])

    # Last attachment in test dataset has no dates
    db_museum_attachment = session.query(MuseumAttachment).get(20)

    assert not db_museum_attachment.created_date
    assert not db_museum_attachment.modified_date


@pytest.mark.usefixtures("mock_iterate_attachments_crash")
def test_sync_attachments_save_progress(
        sync_attachments, session, freeze_time):
    """
    Use --save-progress flag to save progress. Ensure progress is saved
    and used correctly
    """
    freeze_time("2019-02-01")
    sync_attachments(["--save-progress"], success=False)

    # 3 objects were processed before the script stopped
    sync_status = (
        session.query(SyncStatus)
        .filter_by(name="sync_attachments")
        .first()
    )
    assert sync_status.offset == 3
    assert sync_status.start_sync_date == datetime.datetime(
        2019, 2, 1, tzinfo=datetime.timezone.utc
    )

    # Change time; this shouldn't affect anything since we only fetch the date
    # at the start of synchronization run.
    freeze_time("2019-02-02")

    # Run the script three more times to ensure all objects are synced
    for _ in range(0, 2):
        sync_attachments(["--save-progress"], success=False)

    sync_attachments(["--save-progress"])

    sync_status = (
        session.query(SyncStatus)
        .filter_by(name="sync_attachments")
        .first()
    )
    assert sync_status.offset == 0
    assert sync_status.prev_start_sync_date == datetime.datetime(
        2019, 2, 1, tzinfo=datetime.timezone.utc
    )
    assert not sync_status.start_sync_date
    assert session.query(MuseumAttachment).count() == 10


def test_sync_attachments_second_run_filter(
        sync_attachments, session, sync_status_factory, freeze_time):
    """
    Start a second synchronization run and ensure only a subset of objects
    are synchronized
    """
    sync_status_factory(
        name="sync_attachments",
        prev_start_sync_date=datetime.datetime(
            2018, 1, 17, tzinfo=datetime.timezone.utc
        )
    )

    freeze_time("2019-02-02")

    # Only 5 attachments will be synchronized. Out of the 5 to sync,
    # 4 have a newer modification date and one has no modification date at all
    result = sync_attachments(["--save-progress"])

    assert "5 inserts" in result.stdout
    assert session.query(MuseumAttachment).count() == 5

    sync_status = (
        session.query(SyncStatus)
        .filter_by(name="sync_attachments")
        .first()
    )

    assert sync_status.offset == 0
    assert not sync_status.start_sync_date

    # Next synchronization run will iterate entries modified since this date
    assert sync_status.prev_start_sync_date == datetime.datetime(
        2019, 2, 2, tzinfo=datetime.timezone.utc
    )
