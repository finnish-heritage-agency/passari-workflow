import datetime
import enum
import time

from passari_workflow.redis.connection import get_redis_connection


class HeartbeatSource(enum.Enum):
    """
    Source of a heartbeat. The source can be a procedure that is run
    automatically at certain intervals
    """
    SYNC_PROCESSED_SIPS = "sync_processed_sips"
    SYNC_ATTACHMENTS = "sync_attachments"
    SYNC_OBJECTS = "sync_objects"
    SYNC_HASHES = "sync_hashes"


def submit_heartbeat(source):
    """
    Submit heartbeat to indicate that a procedure has been run successfully
    """
    redis = get_redis_connection()
    source = HeartbeatSource(source)

    key = f"heartbeat:{source.value}"

    redis.set(key, int(time.time()))


def get_heartbeats():
    """
    Get a dict containing the last timestamps for all heartbeat sources.

    This can be used to determine which procedures are failing for whatever
    reason.
    """
    redis = get_redis_connection()

    with redis.pipeline() as pipe:
        for source in HeartbeatSource:
            pipe.get(f"heartbeat:{source.value}")

        values = pipe.execute()

    result = {}

    for source, value in zip(HeartbeatSource, values):
        if value is not None:
            value = int(value)
            value = datetime.datetime.fromtimestamp(
                value, datetime.timezone.utc
            )

        result[source] = value

    return result
