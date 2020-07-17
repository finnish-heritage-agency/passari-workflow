import datetime
from collections import namedtuple
from pathlib import Path

import pytest
from passari.exceptions import PreservationError
from passari_workflow.db.models import (FreezeSource, MuseumObject,
                                               MuseumPackage)
from passari_workflow.queue.queues import QueueType, get_queue


class MockMuseumObjectPackage:
    def __init__(self, sip_filename, path):
        self.sip_filename = sip_filename
        self.path = path

    def copy_log_files_to_archive(self, archive_dir):
        """
        Fake log files getting copied
        """
        object_id = Path(self.path).name

        # This doesn't use the exact same path as the "real" archive directory;
        # we're only testing that this method gets called
        archive_dir = Path(archive_dir)
        (archive_dir / str(object_id)).mkdir()


TEST_DATE = datetime.datetime(
    2019, 1, 2, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
)


@pytest.fixture(scope="function")
def create_sip_call():
    """
    Dictionary containing the arguments to the last call to the mocked
    `passari.scripts.create_sip.main` function
    """
    yield {}


@pytest.fixture(scope="function")
def create_sip(monkeypatch, create_sip_call, museum_packages_dir, archive_dir):
    def mock_create_sip(
            object_id, package_dir, sip_id, create_date, modify_date, update):
        # Store the kwargs in 'create_sip_call' to allow tests to check
        # used parameters
        create_sip_call.update({
            "object_id": object_id, "package_dir": package_dir,
            "sip_id": sip_id, "create_date": create_date,
            "modify_date": modify_date, "update": update
        })

        return MockMuseumObjectPackage(
            sip_filename=f"fake_package-{sip_id}.tar",
            path=str(museum_packages_dir / str(object_id))
        )

    def mock_from_path_sync(package_dir, sip_id):
        return MockMuseumObjectPackage(
            sip_filename=f"fake_package-{sip_id}.tar",
            path=str(package_dir)
        )

    monkeypatch.setattr(
        "passari_workflow.jobs.create_sip.main",
        mock_create_sip
    )
    monkeypatch.setattr(
        "passari_workflow.jobs.utils.PACKAGE_DIR",
        str(museum_packages_dir)
    )
    monkeypatch.setattr(
        "passari_workflow.jobs.utils.ARCHIVE_DIR",
        str(archive_dir)
    )
    monkeypatch.setattr(
        "passari_workflow.jobs.utils.MuseumObjectPackage"
        ".from_path_sync",
        mock_from_path_sync
    )

    from passari_workflow.jobs.create_sip import create_sip
    yield create_sip


def test_create_sip(session, create_sip, museum_package, create_sip_call):
    """
    Test running the 'create_sip' workflow job
    """
    museum_package.downloaded = True
    museum_package.created_date = datetime.datetime(
        2019, 1, 2, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
    )
    session.commit()

    create_sip(123456, sip_id="testID")

    # Database should be updated
    db_museum_package = session.query(MuseumPackage).filter_by(
        sip_filename="fake_package-testID.tar"
    ).one()

    # 'create_sip' was called correctly
    assert not create_sip_call["update"]
    assert create_sip_call["create_date"] == datetime.datetime(
        2019, 1, 2, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
    )
    assert not create_sip_call["modify_date"]

    assert db_museum_package.downloaded
    assert db_museum_package.packaged
    assert not db_museum_package.uploaded

    # New job should be enqueued
    queue = get_queue(QueueType.SUBMIT_SIP)
    assert queue.jobs[0].id == "submit_sip_123456"
    assert queue.jobs[0].kwargs == {
        "object_id": 123456, "sip_id": "testID"
    }


def test_update_sip(
        session, create_sip, museum_package, museum_object,
        museum_package_factory, create_sip_call):
    """
    Test running the 'create_sip' workflow job when a SIP has already been
    preserved
    """
    # Create 2 packages that were created before the one we're
    # creating now
    museum_package.sip_filename = "fake_package-testID3.tar"
    museum_package_factory(
        sip_filename="fake_package-testID.tar",
        created_date=datetime.datetime(
            2018, 6, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
        preserved=True,
        museum_object=museum_object
    )
    museum_package_factory(
        sip_filename="fake_package-testID2.tar",
        created_date=datetime.datetime(
            2018, 9, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
        preserved=True,
        museum_object=museum_object
    )
    museum_object.preserved = True
    museum_package.downloaded = True
    museum_package.created_date = datetime.datetime(
        2019, 1, 2, 12, 0, 0, 0, tzinfo=datetime.timezone.utc
    )
    session.commit()

    create_sip(123456, sip_id="testID3")

    # Database should be updated
    db_museum_package = session.query(MuseumPackage).filter_by(
        sip_filename="fake_package-testID3.tar"
    ).one()

    # 'create_sip' was called correctly
    assert create_sip_call["update"]
    # The update is done against the last preserved package
    assert create_sip_call["create_date"].timestamp() == datetime.datetime(
        2018, 9, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc
    ).timestamp()
    assert create_sip_call["modify_date"].timestamp() == datetime.datetime(
        2019, 1, 2, 12, 0, 0, 0, tzinfo=datetime.timezone.utc
    ).timestamp()

    assert db_museum_package.downloaded
    assert db_museum_package.packaged
    assert not db_museum_package.uploaded

    # New job should be enqueued
    queue = get_queue(QueueType.SUBMIT_SIP)
    assert queue.jobs[0].id == "submit_sip_123456"
    assert queue.jobs[0].kwargs == {
        "object_id": 123456, "sip_id": "testID3"
    }


def test_preservation_error(
        session, create_sip, monkeypatch, museum_package, museum_packages_dir,
        archive_dir):
    """
    Test that encountering a PreservationError during a 'create_sip'
    job will freeze the object and remove the object from the workflow
    """
    def mock_create_sip(
            object_id, package_dir, sip_id, create_date, modify_date, update):
        raise PreservationError(
            detail="Mock error message.",
            error="Unsupported file format: wad"
        )

    # Create the fake museum package directory
    (museum_packages_dir / "123456" / "sip").mkdir(parents=True)
    (museum_packages_dir / "123456" / "reports").mkdir(parents=True)

    monkeypatch.setattr(
        "passari_workflow.jobs.create_sip.main",
        mock_create_sip
    )
    museum_package.downloaded = True
    session.commit()

    create_sip(123456, sip_id="testID")

    # Database should be updated
    db_museum_package = session.query(MuseumPackage).filter_by(
        sip_filename="fake_package-testID.tar"
    ).one()
    db_museum_object = session.query(MuseumObject).filter_by(id=123456).one()

    assert db_museum_package.downloaded
    assert not db_museum_package.packaged
    assert not db_museum_package.uploaded
    # The package was cancelled
    assert db_museum_package.cancelled

    assert db_museum_object.frozen
    assert db_museum_object.freeze_reason == "Unsupported file format: wad"
    assert db_museum_object.freeze_source == FreezeSource.AUTOMATIC

    # No new job was enqueued
    queue = get_queue(QueueType.SUBMIT_SIP)
    assert not queue.job_ids

    # The museum package directory was deleted
    assert not (museum_packages_dir / "123456").is_dir()

    # The log file was archived.
    # We only test for the existence of the directory since the actual method
    # is mocked and only creates a directory.
    assert (archive_dir / "123456").is_dir()
