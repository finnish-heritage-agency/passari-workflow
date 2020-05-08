import os
from pathlib import Path

from sqlalchemy.sql import and_, exists

from passari.dpres.package import MuseumObjectPackage
from passari.scripts.submit_sip import main
from passari_workflow.config import PACKAGE_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumPackage
from passari_workflow.jobs.utils import job_locked_by_object_id


@job_locked_by_object_id
def submit_sip(object_id, sip_id):
    """
    Submit SIP to the DPRES service.

    The next workflow task will be enqueued by 'sync_processed_sips' which
    periodically checks the processed SIPs
    """
    object_id = int(object_id)
    connect_db()

    package_dir = Path(PACKAGE_DIR) / str(object_id)

    # Retrieve the latest SIP filename
    museum_package = MuseumObjectPackage.from_path_sync(
        package_dir, sip_id=sip_id
    )
    filename = museum_package.sip_filename

    with scoped_session() as db:
        package_uploaded = db.query(
            exists().where(
                and_(
                    MuseumPackage.sip_filename == museum_package.sip_filename,
                    MuseumPackage.uploaded == True
                )
            )
        ).scalar()
        if package_uploaded:
            raise RuntimeError(f"Package {filename} already uploaded")

    print(f"Submitting {filename} for Object {object_id}")

    museum_package = main(
        object_id=object_id, package_dir=PACKAGE_DIR, sip_id=sip_id
    )

    print(f"Package {filename} submitted, removing local file")

    with scoped_session() as db:
        db_museum_package = db.query(MuseumPackage).filter_by(
            sip_filename=museum_package.sip_filename
        ).one()
        db_museum_package.uploaded = True

    # Delete the generated SIP to free space
    os.remove(museum_package.sip_archive_path)
