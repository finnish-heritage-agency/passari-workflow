from passari_workflow.scripts.enqueue_objects import \
    enqueue_objects as do_enqueue_objects


def enqueue_objects(object_count):
    """
    Enqueue objects using a background RQ job. This is used in when we don't
    want the user action to block.
    """
    do_enqueue_objects(object_count)
