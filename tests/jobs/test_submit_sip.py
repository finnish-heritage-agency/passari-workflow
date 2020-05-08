import datetime
from collections import namedtuple
from pathlib import Path

import pytest
from passari_workflow.db.models import MuseumObject, MuseumPackage

MockMuseumObjectPackage = namedtuple(
    "MockMuseumObjectPackage", ["sip_filename", "sip_archive_path"]
)

TEST_DATE = datetime.datetime(
    2019, 1, 2, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
)


@pytest.fixture(scope="function")
def fake_sip_path(tmpdir):
    package_dir = Path(tmpdir) / "12345678"
    package_dir.mkdir(exist_ok=True)
    package_dir.joinpath("fake_package.tar").touch(exist_ok=True)

    return package_dir.joinpath("fake_package.tar")


@pytest.fixture(scope="function")
def submit_sip(monkeypatch, tmpdir, fake_sip_path):
    def mock_submit_sip(object_id, package_dir, sip_id):
        return MockMuseumObjectPackage(
            sip_filename=f"fake_package-{sip_id}.tar",
            sip_archive_path=str(fake_sip_path)
        )

    def mock_from_path_sync(package_dir, sip_id):
        return MockMuseumObjectPackage(
            sip_filename=f"fake_package-{sip_id}.tar",
            sip_archive_path=str(fake_sip_path)
        )

    monkeypatch.setattr(
        "passari_workflow.jobs.submit_sip.main",
        mock_submit_sip
    )
    monkeypatch.setattr(
        "passari_workflow.jobs.submit_sip.MuseumObjectPackage"
        ".from_path_sync",
        mock_from_path_sync
    )

    from passari_workflow.jobs.submit_sip import submit_sip
    yield submit_sip


def test_submit_sip(session, submit_sip, fake_sip_path, museum_package):
    museum_package.downloaded = True
    museum_package.packaged = True
    session.commit()

    assert fake_sip_path.is_file()

    submit_sip(123456, sip_id="testID")

    db_museum_package = session.query(MuseumPackage).filter_by(
        sip_filename="fake_package-testID.tar"
    ).one()

    assert db_museum_package.downloaded
    assert db_museum_package.packaged
    assert db_museum_package.uploaded

    # SIP is deleted after the command is successful
    assert not fake_sip_path.is_file()


def test_submit_sip_already_uploaded(session, submit_sip, museum_package):
    museum_package.downloaded = True
    museum_package.packaged = True
    museum_package.uploaded = True
    session.commit()

    with pytest.raises(RuntimeError) as exc:
        submit_sip(123456, sip_id="testID")

    assert "already uploaded" in str(exc.value)
