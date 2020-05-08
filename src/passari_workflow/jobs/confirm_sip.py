from pathlib import Path

from passari.dpres.package import MuseumObjectPackage
from passari.scripts.confirm_sip import main
from passari_workflow.config import ARCHIVE_DIR, PACKAGE_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumObject, MuseumPackage


def confirm_sip(object_id, sip_id):
    """
    Confirm SIP that was either preserved or rejected by the DPRES service.
    This is the last step in the preservation workflow.
    """
    object_id = int(object_id)
    connect_db()

    package_dir = Path(PACKAGE_DIR) / str(object_id)

    museum_package = MuseumObjectPackage.from_path_sync(
        package_dir, sip_id=sip_id
    )
    # '.status' file contains either the text 'accepted' or 'rejected'
    status = (
        museum_package.path / f"{museum_package.sip_filename}.status"
    ).read_text()

    if status not in ("accepted", "rejected"):
        raise ValueError(f"Invalid preservation status: {status}")

    print(f"Confirming SIP {museum_package.sip_filename}")
    main(
        object_id=object_id,
        package_dir=PACKAGE_DIR,
        archive_dir=ARCHIVE_DIR,
        sip_id=sip_id,
        status=status
    )

    with scoped_session() as db:
        db.query(MuseumPackage).filter_by(
            sip_filename=museum_package.sip_filename
        ).update({
            MuseumPackage.preserved: bool(status == "accepted"),
            MuseumPackage.rejected: bool(status == "rejected")
        })

        if status == "accepted":
            db.query(MuseumObject).filter_by(id=object_id).update({
                MuseumObject.preserved: True
            })

    print(f"SIP {museum_package.sip_filename} confirmed")
