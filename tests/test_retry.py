import time

import pytest

from taskqueue.queue import TaskQueue


@pytest.fixture
def queue(tmp_path):
    return TaskQueue(str(tmp_path / "test.db"))


def test_failed_task_is_retried(queue):
    task_id = queue.enqueue("flaky", max_retries=3)
    queue.claim_next()
    queue.mark_failed(task_id, "boom", backoff_base=0.01)

    t = queue.get(task_id)
    assert t.status == "pending"
    assert t.attempts == 1
    assert t.error == "boom"


def test_retry_becomes_claimable_after_backoff(queue):
    task_id = queue.enqueue("flaky", max_retries=3)
    queue.claim_next()
    queue.mark_failed(task_id, "boom", backoff_base=0.5)

    assert queue.claim_next() is None  # still backing off
    time.sleep(0.6)
    claimed = queue.claim_next()
    assert claimed.id == task_id
    assert claimed.attempts == 2


def test_task_marked_failed_after_exhausting_retries(queue):
    task_id = queue.enqueue("flaky", max_retries=2)

    for _ in range(2):
        queue.claim_next()
        queue.mark_failed(task_id, "boom", backoff_base=0.01)
        time.sleep(0.02)

    t = queue.get(task_id)
    assert t.status == "failed"
    assert t.attempts == 2


def test_backoff_grows_exponentially(queue):
    task_id = queue.enqueue("flaky", max_retries=5)
    conn = queue.pool.get()

    claimed = queue.claim_next()  # attempts -> 1
    assert claimed is not None
    before = time.time()
    queue.mark_failed(task_id, "boom", backoff_base=2.0)
    row = conn.execute("SELECT available_at FROM tasks WHERE id = ?", (task_id,)).fetchone()
    first_delay = row["available_at"] - before

    time.sleep(first_delay + 0.1)  # let the first backoff actually elapse

    claimed = queue.claim_next()  # attempts -> 2
    assert claimed is not None
    before = time.time()
    queue.mark_failed(task_id, "boom again", backoff_base=2.0)
    row = conn.execute("SELECT available_at FROM tasks WHERE id = ?", (task_id,)).fetchone()
    second_delay = row["available_at"] - before

    assert second_delay > first_delay
