import datetime
import errno

from passari.exceptions import PreservationError
from passari.scripts.download_object import main
from passari_workflow.config import PACKAGE_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import (MuseumAttachment, MuseumObject,
                                        MuseumPackage)
from passari_workflow.db.utils import bulk_create_or_get
from passari_workflow.jobs.create_sip import create_sip
from passari_workflow.jobs.utils import (freeze_running_object,
                                         job_locked_by_object_id)
from passari_workflow.queue.queues import QueueType, get_queue


@job_locked_by_object_id
def download_object(object_id):
    """
    Download an object from MuseumPlus and enqueue the task 'create_sip'
    once the object is downloaded
    """
    object_id = int(object_id)
    connect_db()

    # Create a SIP id from the current time
    sip_id = datetime.datetime.now(
        datetime.timezone.utc
    ).strftime("%Y%m%d-%H%M%S")

    try:
        museum_package = main(
            object_id=int(object_id), package_dir=PACKAGE_DIR,
            # 'sip_id' is optional, but giving it as a kwarg ensures the
            # filename of the SIP is correct before it is created.
            sip_id=sip_id
        )
    except PreservationError as exc:
        # If a PreservationError was raised, freeze the object
        freeze_running_object(
            object_id=object_id,
            sip_id=sip_id,
            freeze_reason=exc.error
        )
        return
    except OSError as exc:
        if exc.errno == errno.ENOSPC:
            raise OSError(
                errno.ENOSPC,
                "Ran out of disk space. This may have happened because the "
                "package directory ran out of space while downloading a "
                "large attachment. Try removing packages from the directory "
                "and trying again by processing less packages at the same "
                "time."
            )

        raise

    filename = museum_package.sip_filename

    with scoped_session() as db:
        db_museum_object = db.query(MuseumObject).filter(
            MuseumObject.id == object_id
        ).one()

        db_package = db.query(MuseumPackage).filter_by(
            sip_filename=filename
        ).first()

        # Get the attachments that currently exist for this object
        # and add them to the new MuseumPackage
        attachment_ids = museum_package.museum_object.attachment_ids
        db_attachments = bulk_create_or_get(
            db, MuseumAttachment, attachment_ids
        )

        if not db_package:
            db_package = MuseumPackage(
                sip_filename=filename,
                sip_id=sip_id,
                object_modified_date=(
                    museum_package.museum_object.modified_date
                ),
                downloaded=True,
                metadata_hash=db_museum_object.metadata_hash,
                attachment_metadata_hash=(
                    db_museum_object.attachment_metadata_hash
                ),
                attachments=db_attachments
            )
            db_package.museum_object = db_museum_object
        else:
            raise EnvironmentError(
                f"Package with filename {filename} already exists"
            )

        db_museum_object.latest_package = db_package

        queue = get_queue(QueueType.CREATE_SIP)
        queue.enqueue(
            create_sip, kwargs={"object_id": object_id, "sip_id": sip_id},
            job_id=f"create_sip_{object_id}"
        )
