"""
Synchronize the metadata hashes for Objects. Metadata hashes are used
to determine which objects need to be preserved again.
"""
import hashlib
from collections import defaultdict

import click
from sqlalchemy.orm import Load, load_only, subqueryload
from sqlalchemy.sql.expression import bindparam

from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import (MuseumAttachment, MuseumObject,
                                               object_attachment_association_table)
from passari_workflow.heartbeat import HeartbeatSource, submit_heartbeat

# Process 2000 objects at a time
CHUNK_SIZE = 2000


def get_museum_objects_and_attachments(db, from_id=0, limit=500):
    """
    Get a list of MuseumObject and MuseumAttachment instances and an
    association map between the two.

    This query is used to avoid the N + 1 problem that happens with SQLAlchemy
    subqueries.

    :param db: SQLAlchemy instance
    :param int from_id: Retrieve objects with higher IDs than this
    :param int limit: How many objects to retrieve at most

    :returns: List of (museum_object, museum_attachments) tuples
    """
    # Retrieve objects
    objects = list(
        db.query(MuseumObject)
        .options(load_only("id", "metadata_hash", "attachment_metadata_hash"))
        .filter(MuseumObject.id > from_id)
        .order_by(MuseumObject.id)
        .limit(limit)
    )
    object_ids = [obj.id for obj in objects]

    # Retrieve object -> attachment associations
    object_and_attach_ids = (
        db.query(object_attachment_association_table)
        .filter(
            object_attachment_association_table.c.museum_object_id.in_(
                object_ids
            )
        )
    )
    object2attachment_ids = defaultdict(list)
    attachment_ids = set()

    for object_id, attachment_id in object_and_attach_ids:
        object2attachment_ids[object_id].append(attachment_id)
        attachment_ids.add(attachment_id)

    # Retrieve attachments
    attachments_by_id = {
        attachment.id: attachment for attachment in
        db.query(MuseumAttachment)
        .options(load_only("id", "metadata_hash"))
        .filter(MuseumAttachment.id.in_(attachment_ids))
    }

    # Build (museum_object, museum_attachments) results
    results = []

    for object_ in objects:
        attachments = [
            attachments_by_id[attachment_id]
            for attachment_id in object2attachment_ids[object_.id]
        ]
        results.append((object_, attachments))

    return results


def iterate_museum_objects_and_attachments(db):
    """
    Iterate MuseumObjects and related MuseumAttachment instances
    from start to end
    """
    current_id = 0
    while True:
        results = get_museum_objects_and_attachments(
            db=db, from_id=current_id, limit=CHUNK_SIZE
        )
        if results:
            for result in results:
                current_id = result[0].id
                yield result
        else:
            # All results iterated
            break


def get_metadata_hash_for_attachments(attachments):
    """
    Calculate a metadata hash from a collection of attachments.

    The hash will change if any of the attachments changes.
    """
    hashes = [attachment.metadata_hash for attachment in attachments]
    # Sort the hashes to make the hash deterministic regardless of order
    hashes.sort()

    data = b"".join([hash_.encode("utf-8") for hash_ in hashes])

    return hashlib.sha256(data).hexdigest()


def sync_hashes():
    """
    Update object entries with latest metadata hashes to determine which
    objects have been changed. This is done after 'sync_objects' and
    'sync_attachments'.
    """
    updated = 0
    skipped = 0
    total = 0

    with scoped_session() as db:
        query = iterate_museum_objects_and_attachments(db)

        all_iterated = False

        while True:
            results = []
            for i in range(0, CHUNK_SIZE):
                try:
                    results.append(next(query))
                except StopIteration:
                    all_iterated = True
                    break

            update_params = []

            for museum_object, museum_attachments in results:
                total += 1

                # Calculate the attachment metadata hash
                if museum_attachments:
                    # Don't calculate the hash if some attachments are
                    # incomplete
                    metadata_incomplete = any(
                        attach.metadata_hash is None
                        for attach in museum_attachments
                    )

                    if metadata_incomplete:
                        skipped += 1
                        continue

                    attachment_metadata_hash = get_metadata_hash_for_attachments(
                        museum_attachments
                    )
                else:
                    attachment_metadata_hash = ""

                if museum_object.attachment_metadata_hash \
                        == attachment_metadata_hash:
                    # Attachment hash hasn't changed, no need to update
                    continue

                updated += 1

                update_params.append({
                    "_id": museum_object.id,
                    "_attachment_metadata_hash": attachment_metadata_hash
                })

            if update_params:
                update_stmt = (
                    MuseumObject.__table__.update()
                    .where(MuseumObject.id == bindparam("_id"))
                    .values({
                        "attachment_metadata_hash":
                            bindparam("_attachment_metadata_hash")
                    })
                )
                db.execute(update_stmt, update_params)

            print(
                f"{total} iterated, {updated} updated and {skipped} skipped "
                "so far"
            )

            if all_iterated:
                break

    submit_heartbeat(HeartbeatSource.SYNC_HASHES)


@click.command()
def cli():
    connect_db()
    sync_hashes()


if __name__ == "__main__":
    cli()
