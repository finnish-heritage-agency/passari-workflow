import pytest

from passari_workflow.scripts.sync_hashes import cli as sync_hashes_cli

from passari_workflow.db.models import MuseumObject


@pytest.fixture(scope="function")
def sync_hashes(cli):
    def func(args, **kwargs):
        return cli(sync_hashes_cli, args, **kwargs)

    return func


def test_sync_hashes(
        sync_hashes, session, museum_object_factory, museum_attachment_factory):
    """
    Sync 3 objects with different properties
    """
    # First object has two attachments which will be completed with
    # a hash
    museum_object_factory(
        id=10,
        attachments=[
            museum_attachment_factory(
                metadata_hash="1568e677140ab834ebdbd98ffa092a273af66084eb04e13b9d07be493847b94f"
            ),
            museum_attachment_factory(
                metadata_hash="a7c4f6c82ab5ed73a359c5d875a9870d899a0642922b6f852539d048676dac74"
            )
        ]
    )

    # Expected hash when SHA256(attachments[0].hash + attachments[1].hash)
    # is done for the museum object
    expected_hash = "be2c3265f2c8f4b05e287ac9fae8a25dad227bda2ebb60ac8dcc929d6b891c27"

    # Second object has no attachments. The metadata hash value for this
    # will be an empty string.
    museum_object_factory(id=20)

    # Third object has attachments, but the third one has one attachment
    # which doesn't have the metadata hash yet. This object will be skipped.
    museum_object_factory(
        id=30,
        attachments=[
            museum_attachment_factory(
                metadata_hash="1568e677140ab834ebdbd98ffa092a273af66084eb04e13b9d07be493847b94f"
            ),
            museum_attachment_factory(metadata_hash=None)
        ]
    )

    sync_hashes([])

    museum_object_a = session.query(MuseumObject).get(10)
    museum_object_b = session.query(MuseumObject).get(20)
    museum_object_c = session.query(MuseumObject).get(30)
    assert museum_object_a.attachment_metadata_hash == expected_hash
    assert museum_object_b.attachment_metadata_hash == ""
    assert museum_object_c.attachment_metadata_hash is None
