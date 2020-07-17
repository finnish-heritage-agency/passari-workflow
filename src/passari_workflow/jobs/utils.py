import shutil
from functools import wraps
from pathlib import Path

import redis_lock
from passari.dpres.package import MuseumObjectPackage
from passari_workflow.config import ARCHIVE_DIR, PACKAGE_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.models import (FreezeSource, MuseumObject,
                                        MuseumPackage)
from passari_workflow.redis.connection import get_redis_connection


def job_locked_by_object_id(func):
    """
    Decorator for a RQ job that ensures that only one job can be running for
    an object ID at the time in any queue.

    This ensures that no race conditions with one RQ job starting just before
    the previous one finishes executing (eg. 'download_object' hasn't finished
    persisting DB update when 'create_sip' starts execution)
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Object ID given in kwargs
            object_id = int(kwargs["object_id"])
        except KeyError:
            # Object ID given in args
            object_id = int(args[0])

        # Use lock named with the object ID to ensure mutual exclusion
        redis = get_redis_connection()
        lock = redis_lock.Lock(redis, f"lock-object-{object_id}")

        with lock:
            return func(*args, **kwargs)

    return wrapper


def freeze_running_object(object_id, sip_id, freeze_reason):
    """
    Cancel and freeze a MuseumObject that is currently in the workflow,
    and mark the SIP as cancelled if one was created.
    """
    with scoped_session() as db:
        museum_object = (
            db.query(MuseumObject)
            .join(
                MuseumPackage,
                MuseumObject.latest_package_id == MuseumPackage.id
            )
            .filter(MuseumObject.id == object_id)
            .one()
        )

        museum_object.frozen = True
        museum_object.freeze_reason = freeze_reason
        museum_object.freeze_source = FreezeSource.AUTOMATIC

        is_same_package = (
            museum_object.latest_package
            and museum_object.latest_package.sip_id == sip_id
        )

        # If package was created, cancel it
        if is_same_package:
            museum_object.latest_package.cancelled = True

        # Copy log files to the archive if they were created
        try:
            museum_package = MuseumObjectPackage.from_path_sync(
                Path(PACKAGE_DIR) / str(object_id), sip_id=sip_id
            )
            museum_package.copy_log_files_to_archive(ARCHIVE_DIR)
        except FileNotFoundError:
            # No object directory and/or log files were created for this
            # package yet
            pass

        try:
            shutil.rmtree(Path(PACKAGE_DIR) / str(object_id))
        except FileNotFoundError:
            # Object directory didn't exist yet
            pass
