from collections import defaultdict
from contextlib import contextmanager
from enum import Enum

import redis_lock
from passari_workflow.redis.connection import get_redis_connection
from rq import Queue
from rq.exceptions import NoSuchJobError
from rq.job import Job
from rq.registry import FailedJobRegistry, StartedJobRegistry


class QueueType(Enum):
    """
    Each queue type corresponds to a RQ queue
    """
    DOWNLOAD_OBJECT = "download_object"
    CREATE_SIP = "create_sip"
    SUBMIT_SIP = "submit_sip"
    CONFIRM_SIP = "confirm_sip"


class WorkflowQueue(Queue):
    # Workflow tasks have a default timeout of 4 hours
    DEFAULT_TIMEOUT = 14400


def job_id_to_object_id(job_id):
    """
    Extract the object ID from a RQ job ID
    """
    try:
        object_id = int(job_id.split("_")[-1])
        return object_id
    except ValueError:
        return None


def get_queue(queue_type):
    """
    Get RQ queue according to its QueueType

    :param QueueType queue_type: Queue to return
    """
    con = get_redis_connection()
    queue_type = QueueType(queue_type)

    queue = WorkflowQueue(queue_type.value, connection=con)

    return queue


def delete_jobs_for_object_id(object_id):
    """
    Delete all jobs for the given object ID
    """
    object_id = int(object_id)
    redis = get_redis_connection()

    cancelled_count = 0

    for queue_type in QueueType:
        job_id = f"{queue_type.value}_{object_id}"
        try:
            Job.fetch(job_id, connection=redis).delete()
            cancelled_count += 1
        except NoSuchJobError:
            pass

    return cancelled_count


def get_enqueued_object_ids():
    """
    Get object IDs from every queue including every pending, executing and
    failed job.

    This can be used to determine which jobs can be enqueued without
    risk of duplicates
    """
    object_ids = set()

    registry_types = (StartedJobRegistry, FailedJobRegistry)

    for queue_type in QueueType:
        queue = get_queue(queue_type)

        # Retrieve started and failed jobs
        for registry_type in registry_types:
            job_registry = registry_type(queue=queue)
            job_ids = job_registry.get_job_ids()

            for job_id in job_ids:
                object_id = job_id_to_object_id(job_id)
                if object_id is not None:
                    object_ids.add(object_id)

        # Retrieve scheduled jobs
        for job_id in queue.get_job_ids():
            object_id = job_id_to_object_id(job_id)
            if object_id is not None:
                object_ids.add(object_id)

    return object_ids


def get_running_object_ids():
    """
    Get object IDs which are currently being executed in the workflow
    """
    object_ids = set()

    for queue_type in QueueType:
        queue = get_queue(queue_type)

        job_registry = StartedJobRegistry(queue=queue)
        job_ids = job_registry.get_job_ids()

        for job_id in job_ids:
            object_id = job_id_to_object_id(job_id)
            if object_id is not None:
                object_ids.add(object_id)

    return object_ids


def get_object_id2queue_map(object_ids):
    """
    Get a {object_id: queue_names} dictionary of object IDs and the queues they
    currently belong to
    """
    queue_object_ids = defaultdict(set)
    queue_map = {}

    for queue_type in QueueType:
        queue = get_queue(queue_type)
        started_registry = StartedJobRegistry(queue=queue)
        job_ids = started_registry.get_job_ids() + queue.get_job_ids()

        # Check pending or executing jobs
        for job_id in job_ids:
            object_id = job_id_to_object_id(job_id)
            if object_id is not None:
                queue_object_ids[queue_type.value].add(object_id)

        failed_registry = FailedJobRegistry(queue=get_queue(queue_type))
        job_ids = failed_registry.get_job_ids()

        # Check failed jobs
        for job_id in job_ids:
            object_id = job_id_to_object_id(job_id)
            if object_id is not None:
                queue_object_ids[queue_type.value].add(object_id)
                queue_object_ids["failed"].add(object_id)

    # Check for all queues plus the catch-all failed queue
    queue_names = [queue_type.value for queue_type in QueueType] + ["failed"]

    for object_id in object_ids:
        queue_map[object_id] = []
        for queue_name in queue_names:
            if object_id in queue_object_ids[queue_name]:
                queue_map[object_id].append(queue_name)

    return queue_map


@contextmanager
def lock_queues():
    """
    Context manager to lock all queues.

    This lock should be acquired when the workflow is affected directly
    (eg. enqueueing new jobs) or indirectly (eg. updating database
    so that changes an object's qualification to be enqueued or not)
    """
    redis = get_redis_connection()

    lock = redis_lock.Lock(redis, "workflow-lock", expire=900)
    lock.acquire(blocking=True)
    try:
        yield lock
    finally:
        lock.release()
