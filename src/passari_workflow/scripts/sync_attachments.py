"""
Synchronize the museum attachments using the MuseumPlus database.

Missing attachments are added and existing entries' metadata hashes
are updated. Museum objects are also linked to the attachments,
with placeholder entries created for any entries that haven't been synced
yet.
"""
import asyncio
import datetime
from collections import defaultdict
from pathlib import Path

import click
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import bindparam

from passari.museumplus.connection import get_museum_session
from passari.museumplus.search import iterate_multimedia
from passari_workflow.config import USER_CONFIG_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumAttachment, MuseumObject
from passari_workflow.db.utils import bulk_create_or_get
from passari_workflow.heartbeat import HeartbeatSource, submit_heartbeat
from passari_workflow.scripts.utils import (finish_sync_progress,
                                                   get_sync_status,
                                                   update_offset)

# How many attachments to retrieve at a time before updating the database
CHUNK_SIZE = 500


async def sync_attachments(offset=0, limit=None, save_progress=False):
    """
    Synchronize attachment metadata from MuseumPlus to determine which
    objects have changed and need to be updated in the DPRES service. This
    is followed by 'sync_hashes'.

    :param int offset: Offset to start synchronizing from
    :param int limit: How many attachments to sync before stopping.
        Default is None, meaning all available attachments
        are synchronized.
    :param bool save_progress: Whether to save synchronization progress
                               and continue from the last run. Offset and limit
                               are ignored if enabled.
    """
    modify_date_gte = None

    if save_progress:
        limit = None

        sync_status = get_sync_status("sync_attachments")
        offset = sync_status.offset
        # Start synchronization from attachments that changed since the last
        # sync
        modify_date_gte = sync_status.prev_start_sync_date
        print(f"Continuing synchronization from {offset}")

    # TODO: This is pretty much an inverse version of 'sync_objects'.
    # This process should be made more generic if possible.
    museum_session = await get_museum_session()
    multimedia_iter = iterate_multimedia(
        session=museum_session, offset=offset,
        modify_date_gte=modify_date_gte
    )
    all_iterated = False
    index = offset
    processed = 0

    while True:
        results = []

        all_iterated = True
        async for result in multimedia_iter:
            all_iterated = False
            results.append(result)
            index += 1

            if len(results) >= CHUNK_SIZE:
                break

        attachments = {result["id"]: result for result in results}
        attachment_ids = list(attachments.keys())

        inserts, updates = 0, 0

        with scoped_session() as db:
            existing_attachment_ids = set([
                result.id for result in
                db.query(MuseumAttachment).options(load_only("id"))
                  .filter(MuseumAttachment.id.in_(attachment_ids))
            ])

            attachment_id2object_id = defaultdict(set)
            object_ids = set()

            update_params = []

            # Create existing objects, update the rest
            for result in attachments.values():
                attachment_id = int(result["id"])
                filename = result["filename"]
                modified_date = result["modified_date"]
                created_date = result["created_date"]
                xml_hash = result["xml_hash"]

                attachment_id2object_id[attachment_id].update(
                    result["object_ids"]
                )
                object_ids.update(result["object_ids"])

                if attachment_id in existing_attachment_ids:
                    # Update
                    update_params.append({
                        "_id": attachment_id,
                        "_filename": filename,
                        "_modified_date": modified_date,
                        "_created_date": created_date,
                        "_metadata_hash": xml_hash
                    })
                    updates += 1
                else:
                    # Create
                    mus_attachment = MuseumAttachment(
                        id=attachment_id,
                        filename=filename,
                        modified_date=modified_date,
                        created_date=created_date,
                        metadata_hash=xml_hash
                    )
                    db.add(mus_attachment)
                    inserts += 1

                processed += 1

                if limit is not None and processed == limit:
                    all_iterated = True
                    break

            if update_params:
                # Perform updates in bulk
                stmt = (
                    MuseumAttachment.__table__.update()
                    .where(MuseumAttachment.id == bindparam("_id"))
                    .values({
                        "filename": bindparam("_filename"),
                        "created_date": bindparam("_created_date"),
                        "modified_date": bindparam("_modified_date"),
                        "metadata_hash": bindparam("_metadata_hash")
                    })
                )
                db.execute(stmt, update_params)

            # Create/update MuseumObjects with references
            # to the newly updated MuseumAttachments.
            # For performance reasons update references for a batch
            # of objects at once
            attachments = (
                db.query(MuseumAttachment)
                .filter(MuseumAttachment.id.in_(attachment_ids))
            )
            objects = bulk_create_or_get(db, MuseumObject, object_ids)
            objects_by_id = {
                mus_object.id: mus_object for mus_object in objects
            }

            for attachment in attachments:
                attachment.museum_objects = [
                    objects_by_id[object_id] for object_id
                    in attachment_id2object_id[attachment.id]
                ]

                for museum_object in attachment.museum_objects:
                    # Set the modification date of MuseumObject to the same
                    # as the attachment's if it's newer.
                    # This is because we want to know if the museum object OR
                    # one of its attachments has been changed.
                    object_date_needs_update = (
                        not museum_object.modified_date
                        or museum_object.modified_date < attachment.modified_date
                    )

                    if object_date_needs_update:
                        museum_object.modified_date = attachment.modified_date

        results = []

        print(
            f"Updated, {inserts} inserts, {updates} "
            f"updates. Updating from offset: {index}"
        )

        # Submit heartbeat after each successful iteration instead of once
        # at the end. This is because this script is designed to be stopped
        # before it has finished iterating everything.
        submit_heartbeat(HeartbeatSource.SYNC_ATTACHMENTS)

        if save_progress:
            update_offset("sync_attachments", offset=index)

        if all_iterated:
            if save_progress:
                finish_sync_progress("sync_attachments")

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
        "efficient updates on subsequent runs. If enabled, --ofset and "
        "--limit are ignored."
    )
)
def cli(offset, limit, save_progress):
    connect_db()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        sync_attachments(
            offset=offset, limit=limit, save_progress=save_progress
        )
    )


if __name__ == "__main__":
    cli()
