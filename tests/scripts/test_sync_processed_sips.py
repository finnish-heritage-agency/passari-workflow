import datetime
import os
import shutil
import time
from pathlib import Path

import freezegun
import pytest
from passari_workflow.db.models import MuseumObject, MuseumPackage
from passari_workflow.queue.queues import QueueType, get_queue
from passari_workflow.scripts.sync_processed_sips import \
    cli as sync_processed_sips_cli

TEST_DATE = datetime.datetime(
    2019, 1, 2, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
)


@pytest.fixture(scope="function")
def sync_processed_sips(cli, museum_packages_dir, monkeypatch):
    def func(args, **kwargs):
        return cli(sync_processed_sips_cli, args, **kwargs)

    monkeypatch.setattr(
        "passari_workflow.scripts.sync_processed_sips.PACKAGE_DIR",
        str(museum_packages_dir)
    )

    return func


@pytest.fixture(scope="function")
def sftp_package_factory(sftp_dir):
    (sftp_dir / "accepted").mkdir()
    (sftp_dir / "rejected").mkdir()

    def func(status, date, object_id, transfer_id, content, sip_id=None):
        if not sip_id:
            sip_id = ""
        else:
            sip_id = f"-{sip_id}"

        # Create directories
        object_dir = (
            sftp_dir / status / date.strftime("%Y-%m-%d")
            / f"20190102_Object_{object_id}{sip_id}.tar"
        )
        transfer_name = \
            f"20190102_Object_{object_id}{sip_id}.tar-{transfer_id}"

        object_dir.mkdir(parents=True, exist_ok=True)

        # Create the reports
        (object_dir / f"{transfer_name}-ingest-report.html").write_text(
            f"<html><body>{content}</body></html>"
        )
        (object_dir / f"{transfer_name}-ingest-report.xml").write_text(
            f"<xml><content>{content}</content></xml>"
        )

        if status == "rejected":
            (object_dir / object_dir.name / "reports").mkdir(
                parents=True, exist_ok=True
            )
            (object_dir / object_dir.name / "reports" / "Object.xml").touch()

        return object_dir

    return func


def test_sync_processed_sips_accepted(
        session, museum_packages_dir, sftp_dir, redis, sftp_package_factory,
        sync_processed_sips):
    # Create local package directory
    museum_packages_dir.joinpath("123456", "logs").mkdir(parents=True)
    museum_packages_dir.joinpath("123456", "sip", "reports").mkdir(
        parents=True
    )

    # Create two accepted SIPs on the mocked SFTP server.
    # The newer one will be selected according to its newer modification date
    new_package_dir = sftp_package_factory(
        status="accepted", date=datetime.datetime(2019, 5, 28),
        object_id=123456, sip_id="AABBCC2", transfer_id="aabbcc",
        content="New report"
    )
    os.utime(
        new_package_dir /
        "20190102_Object_123456-AABBCC2.tar-aabbcc-ingest-report.xml",
        (time.time() - 600, time.time() - 600)
    )

    old_package_dir = sftp_package_factory(
        status="accepted", date=datetime.datetime(2019, 5, 28),
        object_id=123456, sip_id="CCBBAA2", transfer_id="ccbbaa",
        content="Old report"
    )
    os.utime(
        old_package_dir /
        "20190102_Object_123456-CCBBAA2.tar-ccbbaa-ingest-report.xml",
        (time.time() - 1200, time.time() - 1200)
    )

    # Object.xml is required to load MuseumObjectPackage locally
    report_path = Path(__file__).parent.resolve() / "data" / "Object.xml"
    shutil.copyfile(
        report_path,
        museum_packages_dir / "123456" / "sip" / "reports" / "Object.xml"
    )

    db_museum_object = MuseumObject(
        id=123456,
        created_date=TEST_DATE,
        modified_date=TEST_DATE
    )
    db_museum_package = MuseumPackage(
        sip_filename="20190102_Object_123456-AABBCC2.tar",
        sip_id="AABBCC2",
        object_modified_date=TEST_DATE,
        downloaded=True,
        packaged=True,
        uploaded=True,
        museum_object=db_museum_object
    )
    db_museum_object.latest_package = db_museum_package

    session.add(db_museum_object)
    session.commit()

    with freezegun.freeze_time("2019-06-01"):
        result = sync_processed_sips(["--days", "7"])

    assert "Found 2 on 2019-05-28" in result.stdout
    assert "Found 2 accepted SIPs" in result.stdout
    assert "Found 0 rejected SIPs" in result.stdout

    # Ingest reports are downloaded
    assert museum_packages_dir.joinpath(
        "123456", "logs", "ingest-report.xml"
    ).read_text() == "<xml><content>New report</content></xml>"
    assert museum_packages_dir.joinpath(
        "123456", "logs", "ingest-report.html"
    ).read_text() == "<html><body>New report</body></html>"

    # Museum package is updated
    db_museum_package = session.query(MuseumPackage).filter_by(
        sip_filename="20190102_Object_123456-AABBCC2.tar"
    ).one()
    assert db_museum_package.preserved

    # Status file is created
    assert (
        museum_packages_dir / "123456"
        / "20190102_Object_123456-AABBCC2.tar.status"
    ).read_text() == "accepted"

    # RQ task is enqueued
    queue = get_queue(QueueType.CONFIRM_SIP)
    job = queue.jobs[0]
    assert job.id == "confirm_sip_123456"
    assert job.kwargs == {"object_id": 123456, "sip_id": "AABBCC2"}


def test_sync_processed_sips_rejected(
        session, museum_packages_dir, sftp_dir, redis, sftp_package_factory,
        sync_processed_sips, museum_package_factory, museum_object):
    # Create local package directory
    museum_packages_dir.joinpath("123456", "logs").mkdir(parents=True)
    museum_packages_dir.joinpath("123456", "sip", "reports").mkdir(
        parents=True
    )

    sftp_package_factory(
        status="rejected", date=datetime.datetime(2019, 5, 28),
        object_id=123456, transfer_id="aabbcc", content="Rejected report"
    )

    # Object.xml is required to load MuseumObjectPackage locally
    report_path = Path(__file__).parent.resolve() / "data" / "Object.xml"
    shutil.copyfile(
        report_path,
        museum_packages_dir / "123456" / "sip" / "reports" / "Object.xml"
    )

    db_museum_package = museum_package_factory(
        sip_filename="20190102_Object_123456.tar",
        downloaded=True,
        packaged=True,
        uploaded=True,
        museum_object=museum_object
    )
    museum_object.latest_package = db_museum_package
    session.commit()

    with freezegun.freeze_time("2019-06-01"):
        result = sync_processed_sips(["--days", "7"])

    assert "Found 1 on 2019-05-28" in result.stdout
    assert "Found 0 accepted SIPs" in result.stdout
    assert "Found 1 rejected SIPs" in result.stdout

    # Ingest reports are downloaded
    assert museum_packages_dir.joinpath(
        "123456", "logs", "ingest-report.xml"
    ).read_text() == "<xml><content>Rejected report</content></xml>"
    assert museum_packages_dir.joinpath(
        "123456", "logs", "ingest-report.html"
    ).read_text() == "<html><body>Rejected report</body></html>"

    # The server-side SIP is removed, and only the ingest reports remain
    files = list(
        (sftp_dir / "rejected" / "2019-05-28" / "20190102_Object_123456.tar")
        .iterdir()
    )
    assert len(files) == 2
    assert "ingest-report" in files[0].name
    assert "ingest-report" in files[1].name

    # Museum package is updated
    db_museum_package = session.query(MuseumPackage).filter_by(
        sip_filename="20190102_Object_123456.tar"
    ).one()
    assert db_museum_package.rejected
    assert not db_museum_package.preserved

    # Status file is created
    assert (
        museum_packages_dir / "123456" / "20190102_Object_123456.tar.status"
    ).read_text() == "rejected"

    # RQ task is enqueued
    queue = get_queue(QueueType.CONFIRM_SIP)
    assert "confirm_sip_123456" in queue.job_ids
    job = queue.jobs[0]
    assert job.id == "confirm_sip_123456"
    assert job.kwargs == {"object_id": 123456, "sip_id": None}


def test_sync_processed_sips_skipped(
        session, museum_packages_dir, sftp_dir, redis, sftp_package_factory,
        sync_processed_sips, museum_package_factory, museum_object_factory):
    """
    Test that SIPs that have already been marked as preserved or rejected
    will be skipped
    """
    museum_packages = []

    # Create local package directory
    for object_id in range(0, 5):
        (museum_packages_dir / str(object_id) / "logs").mkdir(parents=True)
        (museum_packages_dir / str(object_id) / "sip" / "reports").mkdir(
            parents=True
        )

        sftp_package_factory(
            status="accepted", date=datetime.datetime(2019, 5, 28),
            object_id=object_id, transfer_id="aabbcc",
            content="Accepted report"
        )

        # Object.xml is required to load MuseumObjectPackage locally
        report_path = Path(__file__).parent.resolve() / "data" / "Object.xml"
        shutil.copyfile(
            report_path,
            museum_packages_dir / str(object_id) / "sip" / "reports"
            / "Object.xml"
        )

        museum_object = museum_object_factory(id=object_id)
        db_museum_package = museum_package_factory(
            sip_filename=f"20190102_Object_{object_id}.tar",
            downloaded=True,
            packaged=True,
            uploaded=True,
            museum_object=museum_object
        )
        museum_object.latest_package = db_museum_package
        session.commit()

        museum_packages.append(db_museum_package)

    with freezegun.freeze_time("2019-06-01"):
        result = sync_processed_sips(["--days", 7])

    assert "Found 5 on 2019-05-28" in result.stdout
    assert "Found 5 accepted SIPs" in result.stdout

    # Rollback a single SIP
    museum_packages[1].preserved = False
    (museum_packages_dir / "1"
     / "20190102_Object_1.tar.status").unlink()
    session.commit()

    with freezegun.freeze_time("2019-06-01"):
        result = sync_processed_sips(["--days", 7])

    # Only one will be found since the rest were skipped automatically
    assert "Found 1 on 2019-05-28" in result.stdout
    assert "Found 1 accepted SIPs" in result.stdout

    assert (
        museum_packages_dir / "1"
        / "20190102_Object_1.tar.status"
    ).read_text() == "accepted"

    with freezegun.freeze_time("2019-06-01"):
        result = sync_processed_sips(["--days", 7])

    assert "Found 0 on 2019-05-28" in result.stdout
