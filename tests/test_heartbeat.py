import datetime

from passari_workflow.heartbeat import (HeartbeatSource, get_heartbeats,
                                               submit_heartbeat)


def test_heartbeats():
    """
    Test that heartbeats are generated correctly
    """
    heartbeats = get_heartbeats()

    # No heartbeats have been submitted yet
    assert not heartbeats[HeartbeatSource.SYNC_PROCESSED_SIPS]
    assert not heartbeats[HeartbeatSource.SYNC_OBJECTS]

    # Submit a heartbeat
    submit_heartbeat(HeartbeatSource.SYNC_PROCESSED_SIPS)

    heartbeats = get_heartbeats()

    now = datetime.datetime.now()

    # 'sync_processed_sips' now has a timestamp
    value = heartbeats[HeartbeatSource.SYNC_PROCESSED_SIPS]
    assert value.day == now.day
    assert value.month == now.month

    assert not heartbeats[HeartbeatSource.SYNC_OBJECTS]
