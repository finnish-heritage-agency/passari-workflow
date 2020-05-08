"""
Synchronize the museum objects using the MuseumPlus database.

Missing objects are added and existing objects' metadata hashes are updated
"""
import asyncio
from collections import defaultdict
from pathlib import Path

import click
from sqlalchemy import and_, or_
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import bindparam

from passari.museumplus.connection import get_museum_session
from passari.museumplus.search import iterate_objects
from passari_workflow.config import USER_CONFIG_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumAttachment, MuseumObject
from passari_workflow.db.utils import bulk_create_or_get
from passari_workflow.heartbeat import HeartbeatSource, submit_heartbeat
from passari_workflow.scripts.utils import (finish_sync_progress,
                                                   get_sync_status,
                                                   update_offset)

# How many objects to retrieve at a time before updating the database
CHUNK_SIZE = 500


async def sync_objects(offset=0, limit=None, save_progress=False):
    """
    Synchronize object metadata from MuseumPlus to determine which
    objects have changed and need to be updated in the DPRES service. This
    is followed by 'sync_hashes'.

    :param int offset: Offset to start synchronizing from
    :param int limit: How many objects to sync before stopping.
        Default is None, meaning all available objects are synchronized.
    :param bool save_progress: Whether to save synchronization progress
                               and continue from the last run. Offset and limit
                               are ignored if enabled.
    """
    modify_date_gte = None

    if save_progress:
        limit = None

        sync_status = get_sync_status("sync_objects")
        offset = sync_status.offset
        # Start synchronization from objects that changed since the last
        # sync
        modify_date_gte = sync_status.prev_start_sync_date
        print(f"Continuing synchronization from {offset}")

    museum_session = await get_museum_session()
    object_iter = iterate_objects(
        session=museum_session, offset=offset,
        modify_date_gte=modify_date_gte
    )
    all_iterated = False
    index = offset
    processed = 0

    while True:
        results = []

        all_iterated = True
        async for result in object_iter:
            all_iterated = False
            results.append(result)
            index += 1

            if len(results) >= CHUNK_SIZE:
                break

        objects = {result["id"]: result for result in results}
        object_ids = list(objects.keys())

        inserts, updates = 0, 0

        with scoped_session() as db:
            existing_object_ids = set([
                result.id for result in
                db.query(MuseumObject).options(load_only("id"))
                  .filter(MuseumObject.id.in_(object_ids))
            ])

            object_id2attachment_id = defaultdict(set)
            attachment_ids = set()

            update_params = []

            # Create existing objects, update the rest
            for result in objects.values():
                object_id = int(result["id"])
                title = result["title"]
                modified_date = result["modified_date"]
                created_date = result["created_date"]
                multimedia_ids = result["multimedia_ids"]
                xml_hash = result["xml_hash"]

                object_id2attachment_id[object_id].update(multimedia_ids)
                attachment_ids.update(multimedia_ids)

                if object_id in existing_object_ids:
                    # Don't run the update query instantly; instead,
                    # set the parameters and run them all together later
                    # in bulk
                    update_params.append({
                        "_id": object_id,
                        "_title": title,
                        "_modified_date": modified_date,
                        "_metadata_hash": xml_hash
                    })
                    updates += 1
                else:
                    # Create
                    mus_object = MuseumObject(
                        id=object_id,
                        title=title,
                        modified_date=modified_date,
                        created_date=created_date,
                        metadata_hash=xml_hash
                    )
                    db.add(mus_object)
                    inserts += 1

                processed += 1

                if limit is not None and processed == limit:
                    all_iterated = True
                    break

            if update_params:
                # Perform updates in bulk
                stmt_a = (
                    MuseumObject.__table__.update()
                    .where(MuseumObject.id == bindparam("_id"))
                    .values({
                        "title": bindparam("_title"),
                        "metadata_hash": bindparam("_metadata_hash")
                    })
                )
                stmt_b = (
                    MuseumObject.__table__.update()
                    .where(
                        and_(
                            MuseumObject.id == bindparam("_id"),
                            or_(
                                MuseumObject.modified_date == None,
                                MuseumObject.modified_date
                                < bindparam("_modified_date")
                            )
                        )
                    )
                    .values({
                        "modified_date": bindparam("_modified_date")
                    })
                )
                db.execute(stmt_a, update_params)
                db.execute(stmt_b, update_params)

            # Create/update MuseumAttachments with references
            # to the newly updated MuseumObjects.
            # For performance reasons update references for a batch
            # of objects at once
            objects = (
                db.query(MuseumObject)
                .filter(MuseumObject.id.in_(object_ids))
            )
            attachments = bulk_create_or_get(
                db, MuseumAttachment, attachment_ids
            )
            attachments_by_id = {
                attachment.id: attachment for attachment in attachments
            }

            for museum_object in objects:
                museum_object.attachments = [
                    attachments_by_id[attachment_id] for attachment_id
                    in object_id2attachment_id[museum_object.id]
                ]

        results = []

        print(
            f"Updated, {inserts} inserts, {updates} "
            f"updates. Updating from offset: {index}"
        )

        # Submit heartbeat after each successful iteration instead of once
        # at the end. This is because this script is designed to be stopped
        # before it has finished iterating everything.
        submit_heartbeat(HeartbeatSource.SYNC_OBJECTS)

        if save_progress:
            update_offset("sync_objects", offset=index)

        if all_iterated:
            if save_progress:
                finish_sync_progress("sync_objects")

            break

    await museum_session.close()


@click.command()
@click.option("--offset", default=0)
@click.option("--limit", type=int, default=None)
@click.option(
    "--save-progress/--no-save-progress", is_flag=True, default=False,
    help=(
        "If enabled, synchronization progress will be saved to allow "
        "the run to be continued from an incomplete state and enable more "
        "efficient updates on subsequent runs. If enabled, --offset and "
        "--limit are ignored."
    )
)
def cli(offset, limit, save_progress):
    connect_db()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        sync_objects(
            offset=offset, limit=limit, save_progress=save_progress
        )
    )


if __name__ == "__main__":
    cli()
