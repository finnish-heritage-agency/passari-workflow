import datetime
import enum
import gzip
from pathlib import Path
from typing import List

from sqlalchemy import (BigInteger, Boolean, Column, DateTime, Enum,
                        ForeignKey, Index, MetaData, String, Table, Text,
                        UniqueConstraint, and_, exists, func, not_, or_)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql.functions import coalesce

from passari.dpres.package import get_archive_path_parts
from passari_workflow.config import (ARCHIVE_DIR, PACKAGE_DIR,
                                            PRESERVATION_DELAY, UPDATE_DELAY)

Base = declarative_base(
    metadata=MetaData(
        # Custom 'naming_convention' required to make Alembic migrations
        # easier to maintain:
        # https://alembic.sqlalchemy.org/en/latest/naming.html
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s"
        }
    )
)


class FreezeSource(enum.Enum):
    """
    Enumeration for the source of a freeze.

    Object may be frozen manually by an user or automatically by the workflow.
    """
    USER = "user"
    AUTOMATIC = "automatic"


object_attachment_association_table = Table(
    "object_attachment_association", Base.metadata,
    Column(
        "museum_object_id", BigInteger, ForeignKey("museum_objects.id"),
        index=True
    ),
    Column(
        "museum_attachment_id", BigInteger,
        ForeignKey("museum_attachments.id"),
        index=True
    ),
    UniqueConstraint("museum_object_id", "museum_attachment_id")
)

package_attachment_association_table = Table(
    "package_attachment_association", Base.metadata,
    Column(
        "museum_package_id", BigInteger, ForeignKey("museum_packages.id"),
        index=True
    ),
    Column(
        "museum_attachment_id", BigInteger,
        ForeignKey("museum_attachments.id"),
        index=True
    ),
    UniqueConstraint("museum_package_id", "museum_attachment_id")
)


class MuseumAttachment(Base):
    """
    Museum attachment that may belong to one or more MuseumObjects.

    Attachment corresponds roughly to the Multimedia module in MuseumPlus.
    """
    __tablename__ = "museum_attachments"

    id = Column(BigInteger, primary_key=True)

    filename = Column(Text)
    modified_date = Column(DateTime(timezone=True))
    created_date = Column(DateTime(timezone=True))

    metadata_hash = Column(String(64))

    museum_objects = relationship(
        "MuseumObject",
        secondary=object_attachment_association_table,
        back_populates="attachments"
    )
    packages = relationship(
        "MuseumPackage",
        secondary=package_attachment_association_table,
        back_populates="attachments"
    )


class MuseumPackage(Base):
    """
    Package corresponds to an archive created from a MuseumObject
    at a certain point in time
    """
    __tablename__ = "museum_packages"

    __table_args__ = (
        Index(
            "ix_museum_packages_sip_filename_trgm_gin", "sip_filename",
            postgresql_ops={"sip_filename": "gin_trgm_ops"},
            postgresql_using="gin"
        ),
    )

    id = Column(BigInteger, primary_key=True)
    # Linux has a maximum filename length of 255 characters in most cases
    sip_filename = Column(String(255), index=True, unique=True)

    # SIP ID used to differentiate multiple SIPs generated from the same
    # version of a museum object
    sip_id = Column(String(255))

    # The modification timestamp of the underlying MuseumObject at the time
    # the package was made. NOT the same as the time when this MuseumPackage
    # was created!
    object_modified_date = Column(DateTime(timezone=True))

    created_date = Column(
        DateTime(timezone=True),
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        index=True
    )

    # Hash for the object's metadata at the time of packaging
    metadata_hash = Column(String(64))
    attachment_metadata_hash = Column(String(64))

    downloaded = Column(Boolean, default=False)
    packaged = Column(Boolean, default=False)
    uploaded = Column(Boolean, default=False)

    # Whether the SIP was rejected by the digital preservation service
    rejected = Column(Boolean, default=False)
    preserved = Column(Boolean, default=False)

    # Whether the SIP's creation was cancelled, usually by freezing the
    # underlying MuseumObject
    cancelled = Column(Boolean, default=False, server_default="f")

    museum_object_id = Column(
        BigInteger,
        ForeignKey(
            "museum_objects.id", name="fk_museum_package_museum_object"
        ),
        index=True
    )
    museum_object = relationship(
        "MuseumObject", back_populates="packages",
        foreign_keys=[museum_object_id],
    )
    attachments = relationship(
        "MuseumAttachment",
        secondary=package_attachment_association_table,
        back_populates="packages"
    )

    @property
    def archive_log_dir(self) -> Path:
        """
        Path to the log directory containing the archived logs and reports
        after the package has been processed in the preservation service.
        """
        return (
            Path(ARCHIVE_DIR)
            .joinpath(
                *get_archive_path_parts(
                    object_id=self.museum_object_id,
                    sip_filename=self.sip_filename
                )
            )
            / "logs"
        )

    @property
    def workflow_log_dir(self) -> Path:
        """
        Path to the log directory containing the log and reports
        when the package is in progress and still in the workflow
        """
        return Path(PACKAGE_DIR) / str(self.museum_object_id) / "logs"

    @property
    def log_files_archived(self) -> bool:
        """
        Check whether the log files are archived or still in the workflow

        :returns: True if log files are archived, False if not
        """
        return self.preserved or self.rejected or self.cancelled

    def get_log_filenames(self) -> List[str]:
        """
        Return list of filenames for archived log files
        """
        if self.log_files_archived:
            # Log files are archived and compressed
            return [
                # Log files are compressed and have the '.gz' suffix;
                # remove the suffix here since it's not useful when
                # displaying the list of names in the frontend.
                file_.name[:-3] for file_ in self.archive_log_dir.glob("*")
                if file_.is_file()
            ]
        else:
            # Log files are still in the workflow and uncompressed
            return [
                file_.name for file_ in self.workflow_log_dir.glob("*")
                if file_.is_file()
            ]

    def get_log_file_content(self, filename: str) -> str:
        """
        Return the uncompressed content for a log file as a string
        """
        if self.log_files_archived:
            file_path = self.archive_log_dir / f"{filename}.gz"

            with gzip.open(file_path) as file_:
                return file_.read().decode("utf-8")
        else:
            file_path = self.workflow_log_dir / filename

            with open(file_path, "rb") as file_:
                return file_.read().decode("utf-8")


class MuseumObject(Base):
    """
    Museum object corresponds to an Object in the MuseumPlus
    database
    """
    __tablename__ = "museum_objects"

    __table_args__ = (
        Index(
            "ix_museum_objects_title_trgm_gin", "title",
            postgresql_ops={"title": "gin_trgm_ops"},
            postgresql_using="gin"
        ),
        Index(
            "ix_museum_objects_freeze_reason_trgm_gin", "freeze_reason",
            postgresql_ops={"freeze_reason": "gin_trgm_ops"},
            postgresql_using="gin"
        )
    )

    id = Column(BigInteger, primary_key=True)

    # Human-readable name for the object corresponding to the
    # 'ObjObjectVrt' field in MuseumPlus
    title = Column(Text)

    # Object is preserved if at least one packaged version of it has
    # been accepted in the digital preservation service
    preserved = Column(Boolean, default=False)

    # If the object is frozen, it won't be pending for preservation
    # until it has been unfrozen.
    # Object may be frozen if something is preventing it from being preserved
    # for the time being (eg. one of the file types is not supported yet)
    frozen = Column(Boolean, default=False, index=True)
    freeze_reason = Column(Text)
    freeze_source = Column(postgresql.ENUM(FreezeSource))

    # Creation date of the originating Object module. This can be None.
    created_date = Column(DateTime(timezone=True))

    # This is the modification date of the Object module itself or one of its
    # attachments (aka Multimedia), whichever is the most recent one.
    # Used to determine whether the object needs to be preserved again.
    # This can be None.
    modified_date = Column(DateTime(timezone=True))

    # Object will (eventually) become eliglble for re-preservation if
    # modification date has changed, as well as at least one of the following
    # metadata hashes has changed

    # Hash for the object's XML metadata
    metadata_hash = Column(String(64))
    # Hash for the cumulative metadata of all the object's attachments.
    # This will be None if attachment information is still unknown
    # or an empty string if this object has no attachments.
    attachment_metadata_hash = Column(String(64))

    packages = relationship(
        "MuseumPackage", back_populates="museum_object",
        order_by="MuseumPackage.created_date",
        cascade="all, delete-orphan",
        foreign_keys="MuseumPackage.museum_object_id"
    )
    attachments = relationship(
        "MuseumAttachment",
        secondary=object_attachment_association_table,
        back_populates="museum_objects"
    )

    latest_package_id = Column(
        BigInteger,
        ForeignKey(
            "museum_packages.id", name="fk_museum_object_latest_package"
        ),
        index=True
    )
    # TODO: Changing 'MuseumObject.latest_package' should also affect
    # 'MuseumObject.packages'
    latest_package = relationship(
        "MuseumPackage", foreign_keys=[latest_package_id],
        cascade="all", post_update=True
    )

    @property
    def preservation_pending(self):
        """
        Whether this object is candidate for preservation.
        This either means:

        * that the object has never been preserved and one month has passed
        since its creation
        OR
        * that the object has been preserved, but it has been modified
        since than and one month has passed since the modification date
        of the last preserved package
        OR
        * that the object was cancelled during the last preservation attempt,
        which means that it was eligible for preservation and still is
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        preservation_boundary = now - PRESERVATION_DELAY
        update_boundary = now - UPDATE_DELAY

        return (
            not self.frozen
            and bool(
                self.metadata_hash is not None
                and self.attachment_metadata_hash is not None
            )
            and bool(
                (
                    not self.latest_package
                    and (
                        not self.created_date
                        or self.created_date < preservation_boundary
                    )
                ) or (
                    self.latest_package
                    and self.latest_package.object_modified_date != self.modified_date
                    and (
                        not self.latest_package.object_modified_date
                        or self.latest_package.object_modified_date < update_boundary
                    ) and (
                        self.latest_package.metadata_hash
                        != self.metadata_hash
                        or self.latest_package.attachment_metadata_hash
                        != self.attachment_metadata_hash
                    )
                ) or (
                    self.latest_package and self.latest_package.cancelled
                )
            )
        )


def filter_preservation_pending(q):
    """
    Transform query to only include MuseumObject entries which are
    pending preservation
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    preservation_boundary = now - PRESERVATION_DELAY
    update_boundary = now - UPDATE_DELAY

    return (
        q.outerjoin(
            MuseumPackage,
            MuseumPackage.id == MuseumObject.latest_package_id
        )
        .filter(MuseumObject.frozen == False)
        .filter(
            and_(
                # Metadata hashes can't be incomplete before we start
                # preservation
                MuseumObject.attachment_metadata_hash != None,
                MuseumObject.metadata_hash != None
            )
        )
        .filter(
            or_(
                # Object has never been preserved and has passed the
                # preservation delay (by default one month)...
                and_(
                    MuseumObject.latest_package_id == None,
                    or_(
                        MuseumObject.created_date == None,
                        MuseumObject.created_date < preservation_boundary
                    )
                ),
                # ...OR the object has been preserved at least once,
                # but the preservation delay (by default one month) has
                # already passed since last preservation and the object has
                # changed (object or attachment metadata hash changed since
                # last preservation)
                and_(
                    MuseumObject.latest_package_id != None,
                    # Compare modification dates and check for equality,
                    # including NULL != NULL which evaluates to False here
                    coalesce(MuseumPackage.object_modified_date, datetime.datetime.min)
                    != coalesce(MuseumObject.modified_date, datetime.datetime.min),
                    or_(
                        MuseumPackage.object_modified_date == None,
                        MuseumPackage.object_modified_date < update_boundary
                    ),
                    or_(
                        MuseumObject.metadata_hash
                        != MuseumPackage.metadata_hash,
                        MuseumObject.attachment_metadata_hash
                        != MuseumPackage.attachment_metadata_hash
                    )
                ),
                # ...OR the last package was cancelled, meaning that the
                # preservation can be restarted immediately if it's not frozen
                and_(
                    MuseumObject.latest_package_id != None,
                    MuseumPackage.cancelled == True
                )
            )
        )
    )


def exclude_preservation_pending(q):
    """
    Transform query to exclude MuseumObject entries which are pending
    preservation
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    preservation_boundary = now - PRESERVATION_DELAY
    update_boundary = now - UPDATE_DELAY

    return (
        q.outerjoin(
            MuseumPackage,
            MuseumPackage.id == MuseumObject.latest_package_id
        )
        .filter(
            # If any of the four conditions is true, the object will not
            # be preserved and are thus included in this query:
            or_(
                # 1. Is metadata information still incomplete?
                MuseumObject.metadata_hash == None,
                MuseumObject.attachment_metadata_hash == None,
                # 2. Is the object frozen?
                MuseumObject.frozen,
                # 3. The object hasn't been preserved, but it has been less
                #    than a month passed since the creation of the object?
                and_(
                    MuseumObject.latest_package_id == None,
                    coalesce(
                        MuseumObject.created_date, datetime.datetime.min
                    ) > preservation_boundary
                ),
                # 4. Has the object entered preservation before, but...
                and_(
                    MuseumObject.latest_package_id != None,
                    # ...the package wasn't cancelled, and either...
                    MuseumPackage.cancelled == False,
                    or_(
                        # ...modification date hasn't changed?
                        coalesce(
                            MuseumPackage.object_modified_date,
                            datetime.datetime.min
                        ) == coalesce(
                            MuseumObject.modified_date,
                            datetime.datetime.min
                        ),
                        # ...modification date has changed, but it's been
                        # less than a month?
                        coalesce(
                            MuseumPackage.object_modified_date,
                            datetime.datetime.min
                        ) > update_boundary,
                        # ...metadata hashes haven't changed, indicating no
                        # change has happened?
                        and_(
                            MuseumPackage.metadata_hash
                            == MuseumObject.metadata_hash,
                            MuseumPackage.attachment_metadata_hash
                            == MuseumObject.attachment_metadata_hash
                        )
                    )
                )
            )
        )
    )


MuseumObject.filter_preservation_pending = filter_preservation_pending
MuseumObject.exclude_preservation_pending = exclude_preservation_pending


class SyncStatus(Base):
    """
    Sync status used for synchronization scripts
    """
    __tablename__ = "sync_statuses"

    # Name of the synchronization process
    name = Column(Text, primary_key=True)

    # The date when the current synchronization run was started.
    # This will be moved to `prev_start_sync_date` once the synchronization
    # run has finished.
    start_sync_date = Column(
        DateTime(timezone=True), nullable=True, default=None
    )

    # The date for when the last finished synchronization was started.
    # If this is available, only objects more recent than this are retrieved
    # for synchronization to avoid redundant work.
    prev_start_sync_date = Column(
        DateTime(timezone=True), nullable=True, default=None
    )

    # If last synchronization run was incomplete, synchronization will
    # continue from this offset. Otherwise, start from scratch (aka 0).
    offset = Column(BigInteger, default=0, server_default="0")
