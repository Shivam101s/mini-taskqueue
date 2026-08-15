import time

import pytest

from taskqueue.queue import TaskQueue


@pytest.fixture
def queue(tmp_path):
    return TaskQueue(str(tmp_path / "test.db"))


def test_enqueue_and_claim_roundtrip(queue):
    task_id = queue.enqueue("add", args=[2, 3])
    claimed = queue.claim_next()

    assert claimed.id == task_id
    assert claimed.task_name == "add"
    assert claimed.args == [2, 3]
    assert claimed.status == "running"
    assert claimed.attempts == 1


def test_claim_returns_none_when_empty(queue):
    assert queue.claim_next() is None


def test_claimed_task_is_not_claimed_again(queue):
    queue.enqueue("add", args=[1, 1])
    first = queue.claim_next()
    second = queue.claim_next()

    assert first is not None
    assert second is None


def test_higher_priority_claimed_first(queue):
    low_id = queue.enqueue("job", priority=0)
    high_id = queue.enqueue("job", priority=10)

    claimed = queue.claim_next()
    assert claimed.id == high_id

    claimed = queue.claim_next()
    assert claimed.id == low_id


def test_fifo_within_same_priority(queue):
    first_id = queue.enqueue("job", priority=5)
    second_id = queue.enqueue("job", priority=5)

    assert queue.claim_next().id == first_id
    assert queue.claim_next().id == second_id


def test_mark_done_stores_result(queue):
    task_id = queue.enqueue("add", args=[2, 3])
    queue.claim_next()
    queue.mark_done(task_id, result=5)

    t = queue.get(task_id)
    assert t.status == "done"
    assert t.result == "5"


def test_delayed_task_not_claimable_until_available(queue):
    queue.enqueue("job", delay=1.0)
    assert queue.claim_next() is None

    time.sleep(1.1)
    assert queue.claim_next() is not None


def test_stats_counts_by_status(queue):
    a = queue.enqueue("job")
    queue.enqueue("job")
    queue.claim_next()
    queue.mark_done(a, result=None)

    stats = queue.stats()
    assert stats["done"] == 1
    assert stats["pending"] == 1


def test_queue_persists_across_reconnect(tmp_path):
    db_path = str(tmp_path / "persist.db")
    q1 = TaskQueue(db_path)
    task_id = q1.enqueue("job", args=[42])

    q2 = TaskQueue(db_path)
    claimed = q2.claim_next()
    assert claimed.id == task_id
    assert claimed.args == [42]
