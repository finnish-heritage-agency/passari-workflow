import datetime
from pathlib import Path

from collections import namedtuple

from sqlalchemy.orm import load_only

from passari_workflow.db import scoped_session
from passari_workflow.db.models import SyncStatus


SyncStatusReadOnly = namedtuple(
    "SyncStatusReadOnly",
    ["name", "start_sync_date", "prev_start_sync_date", "offset"]
)


def _get_sync_status(db, name):
    """
    Get the SyncStatus entry or create one if it doesn't exist already
    """
    sync_status = (
        db.query(SyncStatus)
        .filter_by(name=name)
        .first()
    )

    if not sync_status:
        sync_status = SyncStatus(name=name, offset=0)
        db.add(sync_status)

    if not sync_status.start_sync_date:
        # If the sync status doesn't have this date, a new synchronization run
        # is starting. Save the current date; when the synchronization run is
        # finished and a new one is started, we only need to find entries
        # that are newer than this date.
        sync_status.start_sync_date = datetime.datetime.now(
            datetime.timezone.utc
        )

    return sync_status


def get_sync_status(name):
    """
    Load the SyncStatus instance and return it for reading
    """
    with scoped_session() as db:
        sync_status = _get_sync_status(db, name)

        # Return a read-only copy of the sync status to prevent having to deal
        # with a SQLAlchemy session that's not used for anything
        # TODO: Can we do this without having to use a namedtuple?
        return SyncStatusReadOnly(
            name=sync_status.name,
            start_sync_date=sync_status.start_sync_date,
            prev_start_sync_date=sync_status.prev_start_sync_date,
            offset=sync_status.offset
        )


def update_offset(name, offset):
    """
    Update current offset to the database
    """
    with scoped_session() as db:
        sync_status = _get_sync_status(db, name)
        sync_status.offset = offset


def finish_sync_progress(name):
    """
    Finish the current synchronization run.

    This ensures the next synchronization run will only iterate a subset
    of entries from MuseumPlus, improving performance.
    """
    with scoped_session() as db:
        sync_status = _get_sync_status(db, name)

        # Next synchronization will start from beginning
        sync_status.offset = 0
        sync_status.prev_start_sync_date = sync_status.start_sync_date
        sync_status.start_sync_date = None
