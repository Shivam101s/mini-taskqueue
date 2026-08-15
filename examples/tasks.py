"""Example tasks. Point the worker at this module to try the CLI end to end:

    python -m taskqueue.cli --db demo.db enqueue add --args '[2, 3]'
    python -m taskqueue.cli --db demo.db worker --import examples.tasks --concurrency 2
"""

import time

from taskqueue.worker import task


@task()
def add(a, b):
    return a + b


@task()
def slow_echo(message, seconds=1):
    time.sleep(seconds)
    return message


@task(name="flaky")
def sometimes_fails(fail_probability=0.5):
    import random

    if random.random() < fail_probability:
        raise RuntimeError("simulated failure")
    return "ok"
