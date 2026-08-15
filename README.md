# mini-taskqueue

![tests](https://github.com/Shivam101s/mini-taskqueue/actions/workflows/tests.yml/badge.svg)

A small distributed task queue — durable, priority-ordered, retrying with
exponential backoff — with zero external dependencies. No Redis, no
RabbitMQ, no broker to run: the queue lives in a single SQLite file, and
correctness under concurrent workers comes from SQLite's own transaction
locking rather than an in-process mutex.

I built this after working with SQS-backed queues and worker pools at
[Valoi](https://github.com/Shivam101s) — this is an independent, from-scratch
exercise in the same problem (durable work queues, retries, concurrent
consumers), scoped down to something a recruiter can read start to finish in
a few minutes.

## Design

**Storage.** A single `tasks` table: id, task name, JSON-encoded args/kwargs,
priority, status, attempt count, and an `available_at` timestamp used for
both initial scheduling and retry backoff.

**Claiming a task is race-free.** Every claim runs inside a
`BEGIN IMMEDIATE` transaction, which takes SQLite's write lock *before*
reading — so two worker threads (or two separate processes) can never both
select the same pending row and both think they own it. This is tested
directly: [`test_concurrency.py`](tests/test_concurrency.py) races 8 threads
against 200 tasks and asserts every task is claimed exactly once.

**Retries use exponential backoff.** A failed task isn't requeued
immediately — it's rescheduled `backoff_base ** attempts` seconds out, and
only permanently marked `failed` once `max_retries` is exhausted.

**Workers are a thread pool** that poll for claimable work, look the task
name up in a small in-process registry populated by an `@task` decorator
(the same pattern Celery uses), run it, and record the result or failure.

## Usage

```bash
pip install -e .

# define some tasks
cat > tasks.py <<'EOF'
from taskqueue.worker import task

@task()
def add(a, b):
    return a + b
EOF

# enqueue work
taskqueue --db demo.db enqueue add --args '[2, 3]'

# run a worker pool against it
taskqueue --db demo.db worker --import tasks --concurrency 4

# check on a task, or the queue as a whole
taskqueue --db demo.db status <task-id>
taskqueue --db demo.db stats
```

See [`examples/tasks.py`](examples/tasks.py) for a couple of runnable example
tasks, including one that fails intermittently to exercise the retry path.

## Tests

```bash
pip install pytest
pytest tests/ -v
```

15 tests covering the claim/complete lifecycle, priority and FIFO ordering,
persistence across reconnects, retry/backoff timing, and — the part that
actually matters for a queue — concurrent correctness under real threads.

## License

MIT
