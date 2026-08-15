import threading

import pytest

from taskqueue.queue import TaskQueue
from taskqueue.worker import Worker, task


@pytest.fixture
def queue(tmp_path):
    return TaskQueue(str(tmp_path / "test.db"))


def test_concurrent_claims_never_double_claim(queue):
    n_tasks = 200
    for _ in range(n_tasks):
        queue.enqueue("job")

    claimed_ids = []
    lock = threading.Lock()

    def claim_loop():
        while True:
            t = queue.claim_next()
            if t is None:
                return
            with lock:
                claimed_ids.append(t.id)

    threads = [threading.Thread(target=claim_loop) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every task claimed, and none claimed twice.
    assert len(claimed_ids) == n_tasks
    assert len(set(claimed_ids)) == n_tasks


def test_worker_pool_processes_every_task_exactly_once(tmp_path):
    queue = TaskQueue(str(tmp_path / "worker_test.db"))
    results = []
    lock = threading.Lock()

    @task(name="record")
    def record(value):
        with lock:
            results.append(value)
        return value

    n_tasks = 100
    for i in range(n_tasks):
        queue.enqueue("record", args=[i])

    worker = Worker(queue, concurrency=6, poll_interval=0.01)
    worker.run(max_iterations=n_tasks * 2 // 6 + 20)

    assert sorted(results) == list(range(n_tasks))
    assert queue.stats().get("done") == n_tasks
