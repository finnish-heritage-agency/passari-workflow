import datetime
import gzip

from passari_workflow.db import scoped_session
from passari_workflow.db.models import (FreezeSource, MuseumAttachment,
                                               MuseumObject, MuseumPackage)


def assert_preservation_pending_count(query, count):
    """
    Assert that for a query a given number of preservable objects exist,
    and that the rest of the objects are non-preservable
    """
    total_count = query.count()
    assert total_count > 0

    assert (
        query.with_transformation(MuseumObject.filter_preservation_pending)
        .count() == count
    )
    # Rest of the objects must be non-preservable,
    # eg. there can't be objects that are not found by either of the two
    # filters
    assert (
        query.with_transformation(MuseumObject.exclude_preservation_pending)
        .count() == total_count - count
    )


class TestMuseumObject:
    def test_museum_object(self, session):
        mus_object = MuseumObject(
            id=1337,
            preserved=True
        )
        session.add(mus_object)

        # Retrieve it
        mus_object = session.query(
            MuseumObject
        ).filter_by(id=1337).first()

        assert mus_object.preserved

    def test_museum_object_freeze_source(self, session):
        mus_object = MuseumObject(
            id=1337, frozen=True, freeze_source=FreezeSource.USER
        )
        session.add(mus_object)

        assert session.query(MuseumObject).filter(
            MuseumObject.freeze_source == FreezeSource.USER
        ).count() == 1
        assert session.query(MuseumObject).filter(
            MuseumObject.freeze_source == FreezeSource.AUTOMATIC
        ).count() == 0

    def test_museum_object_attachments(self, session):
        mus_attachment_a = MuseumAttachment(
            id=10, filename="testAttachment.tar"
        )
        mus_attachment_b = MuseumAttachment(
            id=20, filename="testAttachment2.tar"
        )
        mus_attachment_c = MuseumAttachment(
            id=30, filename="testAttachment3.tar"
        )

        mus_object_a = MuseumObject(
            id=10,
            attachments=[mus_attachment_a, mus_attachment_b]
        )
        mus_object_b = MuseumObject(
            id=20,
            attachments=[mus_attachment_b, mus_attachment_c]
        )

        session.add(mus_object_a)
        session.add(mus_object_b)
        session.commit()

        mus_object_a = session.query(MuseumObject).get(10)
        mus_object_b = session.query(MuseumObject).get(20)

        assert mus_object_a.attachments[0].filename == "testAttachment.tar"
        assert mus_object_a.attachments[1].filename == "testAttachment2.tar"
        assert mus_object_b.attachments[0].filename == "testAttachment2.tar"
        assert mus_object_b.attachments[1].filename == "testAttachment3.tar"


class TestMuseumPackage:
    def test_packages(self, session):
        mus_object = MuseumObject(
            id=1337,
            preserved=True)

        mus_package_a = MuseumPackage(
            sip_filename="test_one.tar",
            museum_object=mus_object,
        )
        mus_package_b = MuseumPackage(
            sip_filename="test_two.tar",
            museum_object=mus_object,
            created_date=(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(minutes=10)
            )
        )
        session.add_all([mus_package_a, mus_package_b])

        session.commit()

        mus_object = session.query(
            MuseumObject
        ).filter_by(id=1337).first()

        assert mus_object.packages[0].sip_filename == "test_one.tar"
        assert mus_object.packages[1].sip_filename == "test_two.tar"
        assert len(mus_object.packages) == 2

        session.delete(mus_package_b)
        session.commit()

        assert len(mus_object.packages) == 1

        assert mus_object.packages[0].museum_object.id == 1337

    def test_preservation_pending_preserved(self, session):
        # MuseumObject has no packages and is more than 30 days old;
        # therefore it should be preserved
        mus_object = MuseumObject(
            id=1,
            preserved=False,
            modified_date=datetime.datetime.now(
                datetime.timezone.utc
            ),
            created_date=(
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=31)
            ),
            metadata_hash="",
            attachment_metadata_hash=""
        )

        # Add one object pending preservation and one that's not
        session.add(mus_object)
        session.add(
            MuseumObject(
                id=2,
                modified_date=datetime.datetime.now(datetime.timezone.utc),
                created_date=datetime.datetime.now(datetime.timezone.utc),
                metadata_hash="",
                attachment_metadata_hash=""
            )
        )
        session.commit()

        # Find the preserved object
        assert (
            session.query(MuseumObject)
            .with_transformation(MuseumObject.exclude_preservation_pending)
            .one().id == 2
        )

        pk = mus_object.id

        mus_object = (
            session.query(MuseumObject)
            .with_transformation(MuseumObject.filter_preservation_pending)
        ).one()

        assert mus_object.id == pk
        assert mus_object.preservation_pending

        # If the MuseumObject is too recent (newer than 30 days),
        # it won't be preserved, even if it has no packages yet
        mus_object.created_date = datetime.datetime.now(datetime.timezone.utc)
        assert not mus_object.preservation_pending

        session.commit()

        assert_preservation_pending_count(session.query(MuseumObject), 0)

    def test_preservation_pending_preserved_no_date(self, session):
        # If the package has no modification date, it is automatically
        # eligible for preservation
        mus_object = MuseumObject(
            id=1, preserved=False, metadata_hash="",
            attachment_metadata_hash=""
        )

        session.add(mus_object)
        session.commit()

        assert mus_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 1)

    def test_preservation_pending_museum_package(self, session):
        now = datetime.datetime.now(datetime.timezone.utc)

        # MuseumObject has one package already, but it was modified again
        # 35 days later. This needs preservation again.
        mus_object = MuseumObject(
            id=1,
            preserved=True,
            modified_date=now - datetime.timedelta(days=15),
            created_date=now - datetime.timedelta(days=90),
            metadata_hash="new_hash",
            attachment_metadata_hash=""
        )
        mus_package = MuseumPackage(
            sip_filename="fake_package.tar",
            object_modified_date=now - datetime.timedelta(days=50),
            downloaded=True,
            packaged=True,
            uploaded=True,
            metadata_hash="old_hash",
            attachment_metadata_hash=""
        )
        mus_object.packages.append(mus_package)
        mus_object.latest_package = mus_package
        assert mus_object.packages[0] == mus_object.latest_package

        session.add(mus_object)
        session.add(
            MuseumObject(
                id=2, created_date=datetime.datetime.now(datetime.timezone.utc),
                modified_date=datetime.datetime.now(datetime.timezone.utc),
                preserved=True
            )
        )
        session.commit()

        # Check that only the preserved object is found
        assert (
            session.query(MuseumObject)
            .with_transformation(MuseumObject.exclude_preservation_pending)
            .one().id == 2
        )
        assert mus_object.preservation_pending
        assert (
            session.query(MuseumObject)
            .with_transformation(MuseumObject.filter_preservation_pending)
            .one().id == 1
        )

        # If the modification date is still the same, no preservation is needed
        mus_object.modified_date = now - datetime.timedelta(days=50)
        assert not mus_object.preservation_pending

        session.commit()

        assert_preservation_pending_count(session.query(MuseumObject), 0)

    def test_preservation_pending_museum_package_no_date(self, session):
        now = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=50)
        )

        mus_object = MuseumObject(
            id=1, preserved=True,
            metadata_hash="new_hash", attachment_metadata_hash=""
        )
        mus_package = MuseumPackage(
            sip_filename="fake_package.tar",
            downloaded=True,
            packaged=True,
            uploaded=True,
            preserved=True,
            metadata_hash="old_hash",
            attachment_metadata_hash=""
        )
        mus_object.packages.append(mus_package)
        mus_object.latest_package = mus_package

        session.add(mus_object)
        session.commit()

        assert not mus_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 0)

        mus_object.modified_date = now
        session.commit()

        assert mus_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 1)

    def test_preservation_pending_museum_package_frozen(self, session):
        """
        Check the 'preservation_pending' status for a MuseumObject that
        would be eligible for preservation but is frozen
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        mus_object = MuseumObject(
            id=1,
            preserved=True,
            frozen=True,
            modified_date=now - datetime.timedelta(days=15),
            created_date=now - datetime.timedelta(days=90)
        )
        mus_package = MuseumPackage(
            sip_filename="fake_package.tar",
            object_modified_date=now - datetime.timedelta(days=50),
            downloaded=True,
            packaged=True,
            uploaded=True
        )
        mus_object.packages.append(mus_package)
        mus_object.latest_package = mus_package
        assert mus_object.packages[0] == mus_object.latest_package

        session.add(mus_object)
        session.commit()

        assert not mus_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 0)

    def test_preservation_pending_museum_package_metadata_hash(
            self, session, museum_object_factory, museum_package_factory):
        """
        Test that museum object is eligible for preservation when either of
        the metadata hashes has changed since the last preserved package
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        museum_object = museum_object_factory(
            id=10,
            created_date=now - datetime.timedelta(days=60),
            modified_date=now - datetime.timedelta(days=15),
            preserved=True,
            metadata_hash="hash1",
            attachment_metadata_hash="aHash1"
        )
        museum_package = museum_package_factory(
            sip_filename="test.tar",
            preserved=True,
            object_modified_date=now - datetime.timedelta(days=50),
            metadata_hash="hash1",
            attachment_metadata_hash="aHash1"
        )
        museum_object.latest_package = museum_package

        session.commit()

        assert not museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 0)

        # Changing the metadata hash makes the object preservable again
        museum_object.metadata_hash = "hash2"
        session.commit()

        assert museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 1)

        # Changing the attachment metadata hash does the same thing
        museum_object.metadata_hash = "hash1"
        museum_object.attachment_metadata_hash = "aHash2"
        session.commit()

        assert museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 1)

    def test_preservation_pending_museum_package_metadata_hash_missing(
            self, session, museum_object_factory, museum_package_factory):
        """
        Test that museum object doesn't become eligible for preservation
        until both of its metadata hash fields have been populated
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        museum_object = museum_object_factory(
            id=10,
            created_date=now - datetime.timedelta(days=60),
            modified_date=now - datetime.timedelta(days=60),
            preserved=True,
        )

        session.commit()

        assert not museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 0)

        # Changing one of the fields alone won't make it preservable
        museum_object.metadata_hash = "hash"
        session.commit()

        assert not museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 0)

        # Once both of the fields are populated, the object becomes preservable
        museum_object.attachment_metadata_hash = "aHash"
        session.commit()

        assert museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 1)

    def test_log_files_archived(self, session, museum_package):
        """
        Check that archived log files can be accessed using a MuseumPackage
        instance
        """
        # Package must be either preserved or rejected in order to have
        # archived logs
        museum_package.preserved = True
        session.commit()

        (museum_package.archive_log_dir).mkdir(parents=True)
        (museum_package.archive_log_dir / "create-sip.log.gz").write_bytes(
            gzip.compress("SIP created".encode("utf-8"))
        )
        (museum_package.archive_log_dir / "ingest-report.html.gz").write_bytes(
            gzip.compress("<html><p>SIP accepted</p></html>".encode("utf-8"))
        )

        assert set(museum_package.get_log_filenames()) == set([
            "create-sip.log", "ingest-report.html"
        ])

        assert museum_package.get_log_file_content("create-sip.log") == \
            "SIP created"
        assert museum_package.get_log_file_content("ingest-report.html") == \
            "<html><p>SIP accepted</p></html>"

    def test_log_files_workflow(self, session, museum_package):
        """
        Check that log files in the workflow can be accessed using a
        MuseumPackage instance
        """
        (museum_package.workflow_log_dir).mkdir(parents=True)
        (museum_package.workflow_log_dir / "create-sip.log").write_text(
            "SIP created"
        )
        (museum_package.workflow_log_dir / "ingest-report.html").write_text(
            "<html><p>SIP accepted</p></html>"
        )

        assert set(museum_package.get_log_filenames()) == set([
            "create-sip.log", "ingest-report.html"
        ])

        assert museum_package.get_log_file_content("create-sip.log") == \
            "SIP created"
        assert museum_package.get_log_file_content("ingest-report.html") == \
            "<html><p>SIP accepted</p></html>"

    def test_preservation_pending_preservation_delay(
            self, session, museum_object, monkeypatch):
        """
        Test that configuring the preservation delay affects
        'preservation pending' tests
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        museum_object.created_date = now - datetime.timedelta(days=5)
        session.commit()

        # Default is 30 days, so the museum object is not pending preservation
        # currently
        assert not museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 0)

        monkeypatch.setattr(
            "passari_workflow.db.models.PRESERVATION_DELAY",
            datetime.timedelta(seconds=4*86400)
        )

        # Delay is 4 days, so the museum object is pending preservation now
        assert museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 1)

    def test_preservation_pending_update_delay(
            self, session, museum_package, monkeypatch):
        """
        Test that configuration the update delay affects 'preservation pending'
        tests
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        museum_object = museum_package.museum_object
        museum_package.metadata_hash = museum_object.metadata_hash
        museum_package.attachment_metadata_hash = \
            museum_object.attachment_metadata_hash
        museum_package.preserved = True
        museum_package.created_date = now - datetime.timedelta(days=5)
        museum_package.object_modified_date = now - datetime.timedelta(days=10)

        museum_object.created_date = now - datetime.timedelta(days=60)
        museum_object.modified_date = now - datetime.timedelta(days=5)
        museum_object.metadata_hash = "new_hash"

        session.commit()

        # Default is 30 days; no re-preservation yet
        assert not museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 0)

        monkeypatch.setattr(
            "passari_workflow.db.models.UPDATE_DELAY",
            datetime.timedelta(seconds=4*86400)
        )

        # Value is 4 days now; re-preservation is now possible
        assert museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 1)

    def test_preservation_pending_cancelled(
            self, session, museum_package, monkeypatch):
        """
        Test that MuseumObject with a cancelled latest package is immediately
        eligible for preservation
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        museum_object = museum_package.museum_object

        museum_package.metadata_hash = museum_object.metadata_hash
        museum_package.attachment_metadata_hash = \
            museum_object.attachment_metadata_hash
        museum_package.cancelled = True
        museum_package.created_date = now - datetime.timedelta(days=5)
        museum_package.object_modified_date = now - datetime.timedelta(days=60)

        museum_object.created_date = now - datetime.timedelta(days=60)
        museum_object.modified_date = now - datetime.timedelta(days=5)
        museum_object.latest_package = museum_package

        session.commit()

        # Museum package was cancelled, so it's immediately available for
        # preservation as long as it's not frozen
        assert museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 1)

        museum_package.cancelled = False
        session.commit()

        # Latest museum package is no longer cancelled, so preservation
        # is not possible until a change is detected
        assert not museum_object.preservation_pending
        assert_preservation_pending_count(session.query(MuseumObject), 0)
