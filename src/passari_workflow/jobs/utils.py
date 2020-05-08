import redis_lock
from passari_workflow.redis.connection import get_redis_connection
from functools import wraps


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
