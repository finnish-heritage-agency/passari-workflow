from passari_workflow.queue.queues import (QueueType,
                                                  delete_jobs_for_object_id,
                                                  get_enqueued_object_ids,
                                                  get_object_id2queue_map,
                                                  get_queue)
from rq import SimpleWorker


def successful_job():
    return "yes yes yes yes! YES!"


def failing_job():
    raise RuntimeError("no no no no!")


def test_get_enqueued_object_ids(redis):
    queue = get_queue(QueueType.CREATE_SIP)

    # Complete two jobs
    queue.enqueue(successful_job, job_id="create_sip_124578")
    queue.enqueue(failing_job, job_id="create_sip_998877")
    SimpleWorker([queue], connection=queue.connection).work(burst=True)

    # Don't finish this job
    queue.enqueue(successful_job, job_id="create_sip_555555")

    # Pending and failed object IDs should all be found
    # Finished job ID won't be included
    object_ids = get_enqueued_object_ids()
    assert 124578 not in object_ids
    assert 998877 in object_ids
    assert 555555 in object_ids


def test_delete_jobs_for_object_id(redis):
    queue_a = get_queue(QueueType.DOWNLOAD_OBJECT)
    queue_b = get_queue(QueueType.SUBMIT_SIP)

    queue_a.enqueue(successful_job, job_id="download_object_123456")
    queue_b.enqueue(failing_job, job_id="submit_sip_123456")
    SimpleWorker([queue_b], connection=queue_b.connection).work(burst=True)

    # Both the pending and failed jobs should be cancelled
    assert delete_jobs_for_object_id(123456) == 2

    assert len(queue_a.job_ids) == 0
    assert len(queue_b.job_ids) == 0

    # Second run does nothing
    assert delete_jobs_for_object_id(123456) == 0


def test_get_object_id2queue_map(redis):
    """
    Test that 'get_object_id2queue_map' returns a correct dictionary
    """
    queue_a = get_queue(QueueType.DOWNLOAD_OBJECT)
    queue_b = get_queue(QueueType.SUBMIT_SIP)

    queue_a.enqueue(successful_job, job_id="download_object_123456")
    queue_b.enqueue(failing_job, job_id="submit_sip_654321")
    SimpleWorker([queue_b], connection=queue_b.connection).work(burst=True)

    queue_map = get_object_id2queue_map([123456, 654321, 111111])
    assert queue_map[123456] == ["download_object"]
    assert queue_map[654321] == ["submit_sip", "failed"]
    assert queue_map[111111] == []
