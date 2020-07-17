import errno

from passari.exceptions import PreservationError
from passari.scripts.create_sip import main
from passari_workflow.config import PACKAGE_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumObject, MuseumPackage
from passari_workflow.jobs.submit_sip import submit_sip
from passari_workflow.jobs.utils import (freeze_running_object,
                                         job_locked_by_object_id)
from passari_workflow.queue.queues import QueueType, get_queue


@job_locked_by_object_id
def create_sip(object_id, sip_id):
    """
    Create SIP from a downloaded objec and enqueue the task 'submit_sip'
    once the object is packaged into a SIP
    """
    object_id = int(object_id)
    connect_db()

    # Are we creating a SIP for the first time or updating a preserved
    # package?
    created_date, modified_date = None, None
    with scoped_session() as db:
        last_preserved_package = (
            db.query(MuseumPackage)
            .filter(MuseumPackage.museum_object_id == object_id)
            .filter(MuseumPackage.preserved == True)
            .order_by(MuseumPackage.created_date.desc())
            .first()
        )
        current_package = (
            db.query(MuseumObject)
            .join(
                MuseumPackage,
                MuseumObject.latest_package_id == MuseumPackage.id
            )
            .filter(MuseumObject.id == object_id)
            .one()
            .latest_package
        )

        if not last_preserved_package:
            # We haven't created a preserved SIP yet
            print(f"Creating submission SIP for Object {object_id}")
            created_date = current_package.created_date
        else:
            # We are updating an existing package
            print(f"Creating update SIP for Object {object_id}")
            created_date = last_preserved_package.created_date
            modified_date = current_package.created_date

    # Run the 'create_sip' script
    try:
        museum_package = main(
            object_id=object_id, package_dir=PACKAGE_DIR, sip_id=sip_id,
            create_date=created_date, modify_date=modified_date,
            update=bool(modified_date)
        )
    except PreservationError as exc:
        # If a PreservationError was raised, freeze the object and prevent
        # the object from going further in the workflow.
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

    print(f"Created SIP for Object {object_id}, updating database")

    with scoped_session() as db:
        db_package = db.query(MuseumPackage).filter(
            MuseumPackage.sip_filename == filename
        ).one()
        db_package.packaged = True
        db.query(MuseumObject).filter(
            MuseumObject.id == object_id
        ).update({MuseumObject.latest_package_id: db_package.id})

        queue = get_queue(QueueType.SUBMIT_SIP)
        queue.enqueue(
            submit_sip, kwargs={"object_id": object_id, "sip_id": sip_id},
            job_id=f"submit_sip_{object_id}"
        )
