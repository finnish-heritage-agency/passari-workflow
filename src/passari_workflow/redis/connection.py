from passari_workflow.config import CONFIG

from redis import Redis


def get_redis_connection(db=0):
    """
    Get Redis connection used for the workflow, distributed locks and other
    miscellaneous tasks
    """
    password = CONFIG["redis"].get("password", None)
    redis = Redis(
        host=CONFIG["redis"]["host"],
        port=CONFIG["redis"]["port"],
        db=db,
        password=password if password else None
    )

    return redis
