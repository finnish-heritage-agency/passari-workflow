"""
Discover accepted AIPs and rejected SIPs by crawling the SFTP `accepted`
and `rejected` directories
"""
import datetime
import os
import os.path
from collections import OrderedDict, namedtuple
from pathlib import Path

import click
from sqlalchemy.sql import and_, or_, select

from passari.dpres.package import MuseumObjectPackage
from passari.dpres.ssh import connect_dpres_sftp
from passari_workflow.config import PACKAGE_DIR
from passari_workflow.db import scoped_session
from passari_workflow.db.connection import connect_db
from passari_workflow.db.models import MuseumObject, MuseumPackage
from passari_workflow.heartbeat import HeartbeatSource, submit_heartbeat
from passari_workflow.jobs.confirm_sip import confirm_sip
from passari_workflow.queue.queues import QueueType, get_queue

SIPResult = namedtuple(
    "SIPResult",
    [
        "sip_filename", "report_path", "report_time", "transfer_name",
        "transfer_path", "status"
    ]
)


def sftp_rmtree(sftp, path):
    """
    Recursive version of rmdir that deletes all files and subdirectories from
    a SFTP path
    """
    files = sftp.listdir(str(path))

    for name in files:
        file_path = os.path.join(path, name)
        try:
            # Try deleting it as a file first
            sftp.remove(file_path)
        except IOError:
            # IOError is only raised when it's a directory
            sftp_rmtree(sftp, file_path)

    sftp.rmdir(str(path))


def update_sip(sip, sftp, queue):
    """
    Update a single SIP by downloading its ingest reports and enqueing the
    final task to confirm it
    """
    with scoped_session() as db:
        db_museum_package = (
            db.query(MuseumPackage).join(
                MuseumObject, MuseumObject.id == MuseumPackage.museum_object_id
            ).filter(
                and_(
                    MuseumPackage.sip_filename == sip.sip_filename,
                    MuseumPackage.preserved == False,
                    MuseumPackage.rejected == False
                )
            ).one_or_none()
        )

        if not db_museum_package:
            return

        if sip.status == "accepted":
            # Package was accepted
            db_museum_package.preserved = True
        elif sip.status == "rejected":
            db_museum_package.rejected = True

        object_id = db_museum_package.museum_object.id
        package_dir = Path(PACKAGE_DIR) / str(object_id)
        museum_package = MuseumObjectPackage.from_path_sync(package_dir)

        xml_temp_path = museum_package.log_dir / "ingest-report.xml.download"
        xml_report_path = museum_package.log_dir / "ingest-report.xml"

        # HTML report also exists with the same path and name, but different
        # suffix
        html_remote_path = sip.report_path.with_suffix(".html")

        html_temp_path = museum_package.log_dir / "ingest-report.html.download"
        html_report_path = museum_package.log_dir / "ingest-report.html"

        # Download ingest report to the log directory
        sftp.get(
            str(sip.report_path),
            str(xml_temp_path)
        )
        os.rename(xml_temp_path, xml_report_path)

        sftp.get(
            str(html_remote_path),
            str(html_temp_path)
        )
        os.rename(html_temp_path, html_report_path)

        # Remove the directory containing the rejected SIP so that the DPRES
        # service does not store the package unnecessarily
        if sip.status == "rejected":
            sftp_rmtree(sftp, sip.transfer_path)

        # Write the status for use by the 'confirm_sip' task
        (package_dir / f"{sip.sip_filename}.status").write_text(sip.status)

        # Enqueue the final task
        queue.enqueue(
            confirm_sip,
            kwargs={
                "object_id": object_id, "sip_id": db_museum_package.sip_id
            },
            job_id=f"confirm_sip_{object_id}"
        )


def update_sips(sip_results, sftp):
    """
    Update processed SIPs one-by-one
    """
    # TODO: We could process SIPs in chunks to reduce DB load
    # (eg. 50 SIPs per DB session). However, this requires a bit more
    # complexity and may not be necessary performance-wise.
    queue = get_queue(QueueType.CONFIRM_SIP)

    for sip in sip_results:
        update_sip(sip, sftp=sftp, queue=queue)


def combine_results(*sip_result_lists):
    """
    Combine the SIPResult lists into a list while removing older results
    """
    results = OrderedDict()

    for sip_results in sip_result_lists:
        for sip_result in sip_results:
            filename = sip_result.sip_filename

            if filename not in results:
                results[filename] = sip_result
                continue

            newer_result = (
                sip_result.sip_filename in results
                and (
                    sip_result.report_time
                    > results[filename].report_time
                )
            )
            if newer_result:
                results[filename] = sip_result

    return list(results.values())


def get_processed_sips(
        sftp, status: str, days: int, confirmed_sip_filenames: set):
    """
    Get a list of processed SIPs from the DPRES service

    :param status: Status ('accepted' or 'rejected') determining which
                   directory to scrape
    :param days: How many days to scrape
    :param confirmed_sip_filenames: Set of filenames to filter confirmed SIPs
    """
    today = datetime.datetime.now(datetime.timezone.utc)

    status_dir = Path(status)
    dirs = sftp.listdir(str(status_dir))

    results = []

    for i in range(0, days):
        date = today - datetime.timedelta(days=i)
        date_dir = date.strftime("%Y-%m-%d")

        if date_dir not in dirs:
            continue

        sip_filenames = sftp.listdir(str(status_dir / date_dir))
        sip_filenames = [
            filename for filename in sip_filenames
            if filename not in confirmed_sip_filenames
        ]
        found_sips = 0

        for sip_filename in sip_filenames:
            transfers = sftp.listdir(
                str(status_dir / date_dir / sip_filename)
            )
            transfers = [
                transfer[:-18] for transfer in transfers
                if transfer.endswith("-ingest-report.xml")
            ]

            for transfer in transfers:
                ingest_report_name = f"{transfer}-ingest-report.xml"

                report_path = (
                    status_dir / date_dir / sip_filename / ingest_report_name
                )
                # Retrieve the date to differentiate between the older
                # and newer uploads of the same SIP
                # We will remove all but the newest upload later on
                # TODO: For now this is done just in case we ever upload
                # a SIP multiple times, which shouldn't happen in practice.
                #
                # If it *does* happen, and the SIP has been confirmed in the
                # database already, then the newer version will be skipped
                # due to the 'skip confirmed SIP directories' optimization
                ingest_report_time = sftp.lstat(str(report_path)).st_mtime
                results.append(
                    SIPResult(
                        sip_filename=sip_filename,
                        report_path=(
                            status_dir / date_dir / sip_filename
                            / ingest_report_name
                        ),
                        report_time=ingest_report_time,
                        transfer_name=transfer,
                        transfer_path=(
                            # Store the path to the original SIP if available
                            status_dir / date_dir / sip_filename / sip_filename
                            if status == "rejected" else None
                        ),
                        status=status
                    )
                )
                found_sips += 1

        print(f"Found {found_sips} on {date_dir}")

    return results


def get_confirmed_sip_filenames(days: int) -> set:
    """
    Get a set of SIP filenames that have already been marked as preserved or
    rejected in the workflow.

    The SIPs can be safely skipped as they're either already confirmed or the
    corresponding workflow job has been enqueued.
    """
    # Find packages that are at most (days + 2) days old. The extra two days
    # are used to account for days that took longer to get processed for
    # whatever reason.
    cutoff = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(days=days + 2)
    )

    with scoped_session() as db:
        query = (
            select([MuseumPackage.sip_filename])
            .where(MuseumPackage.created_date > cutoff)
            .where(or_(MuseumPackage.preserved, MuseumPackage.rejected))
        )
        results = db.execute(query)
        results = {result[0] for result in results}

    return results


def sync_processed_sips(days):
    """
    Synchronize processed SIPs from the DPRES service, mark the corresponding
    packages as either preserved or rejected and cleanup the remaining files
    """
    connect_db()

    confirmed_sip_filenames = get_confirmed_sip_filenames(days)

    with connect_dpres_sftp() as sftp:
        accepted_sips = get_processed_sips(
            sftp, status="accepted", days=days,
            confirmed_sip_filenames=confirmed_sip_filenames
        )
        print(f"Found {len(accepted_sips)} accepted SIPs")

        rejected_sips = get_processed_sips(
            sftp, status="rejected", days=days,
            confirmed_sip_filenames=confirmed_sip_filenames
        )
        print(f"Found {len(rejected_sips)} rejected SIPs")

        completed_sips = combine_results(accepted_sips, rejected_sips)

        update_sips(completed_sips, sftp=sftp)

        submit_heartbeat(HeartbeatSource.SYNC_PROCESSED_SIPS)


@click.command()
@click.option(
    # According to DPRES API docs, reports for rejected packages will preserved
    # for at least 10 days
    "--days", default=31, help="Amount of days to search through")
def cli(days):
    sync_processed_sips(days)


if __name__ == "__main__":
    cli()
