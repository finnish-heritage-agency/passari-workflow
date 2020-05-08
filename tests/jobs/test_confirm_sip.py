from collections import namedtuple
from pathlib import Path

from passari_workflow.db.models import MuseumPackage, MuseumObject

import pytest

MockMuseumObjectPackage = namedtuple(
    "MockMuseumObjectPackage", ["path", "sip_filename", "sip_archive_path"]
)


@pytest.fixture(scope="function")
def fake_sip_path(tmpdir):
    package_dir = Path(tmpdir) / "123456"
    package_dir.mkdir(exist_ok=True)
    package_dir.joinpath("fake_package-testID.tar").touch(exist_ok=True)

    return package_dir.joinpath("fake_package-testID.tar")


@pytest.fixture(scope="function")
def confirm_sip(monkeypatch, tmpdir, fake_sip_path):
    def mock_confirm_sip(object_id, package_dir, archive_dir, status, sip_id):
        return True

    def mock_from_path_sync(package_dir, sip_id):
        return MockMuseumObjectPackage(
            path=fake_sip_path.parent,
            sip_filename=f"fake_package-{sip_id}.tar",
            sip_archive_path=str(fake_sip_path)
        )

    monkeypatch.setattr(
        "passari_workflow.jobs.confirm_sip.main",
        mock_confirm_sip
    )

    monkeypatch.setattr(
        "passari_workflow.jobs.submit_sip.MuseumObjectPackage"
        ".from_path_sync",
        mock_from_path_sync
    )

    from passari_workflow.jobs.confirm_sip import confirm_sip
    yield confirm_sip



@pytest.mark.parametrize("status", ["accepted", "rejected"])
def test_confirm_sip(
        session, confirm_sip, museum_object, museum_package, fake_sip_path,
        status):
    """
    Test that a confirmed SIP is updated correctly in the database
    """
    # Add a status file
    (fake_sip_path.parent / "fake_package-testID.tar.status").write_text(
        status
    )
    confirm_sip(object_id=123456, sip_id="testID")

    # Database was updated
    db_museum_package = (
        session.query(MuseumPackage)
        .filter_by(museum_object_id=123456)
        .one()
    )
    db_museum_object = session.query(MuseumObject).get(123456)

    if status == "accepted":
        assert db_museum_package.preserved
        assert not db_museum_package.rejected

        assert db_museum_object.preserved
    elif status == "rejected":
        assert db_museum_package.rejected
        assert not db_museum_package.preserved

        assert not db_museum_object.preserved


def test_invalid_status(session, confirm_sip, museum_object, fake_sip_path):
    """
    Test that a SIP with an invalid status isn't confirmed
    """
    (fake_sip_path.parent / "fake_package-testID.tar.status").write_text(
        "invalid"
    )
    with pytest.raises(ValueError) as exc:
        confirm_sip(object_id=123456, sip_id="testID")

    assert "Invalid preservation status: invalid" in str(exc.value)
