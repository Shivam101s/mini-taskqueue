import logging
import threading
import time
import traceback
from typing import Callable

from .queue import TaskQueue

logger = logging.getLogger("taskqueue")

_REGISTRY: dict[str, Callable] = {}


def task(name: str = None):
    """Decorator that registers a function as a task the worker pool can run."""

    def decorator(fn: Callable) -> Callable:
        _REGISTRY[name or fn.__name__] = fn
        return fn

    return decorator


class Worker:
    def __init__(self, queue: TaskQueue, concurrency: int = 4, poll_interval: float = 0.2):
        self.queue = queue
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self._stop = threading.Event()

    def run(self, max_iterations: int = None):
        """Run `concurrency` worker threads until stop() is called (or, for
        tests, until `max_iterations` claim attempts have been made per thread).
        """
        threads = [
            threading.Thread(target=self._loop, args=(max_iterations,), daemon=True)
            for _ in range(self.concurrency)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def stop(self):
        self._stop.set()

    def _loop(self, max_iterations: int = None):
        iterations = 0
        while not self._stop.is_set():
            if max_iterations is not None and iterations >= max_iterations:
                return
            iterations += 1

            task_obj = self.queue.claim_next()
            if task_obj is None:
                time.sleep(self.poll_interval)
                continue

            fn = _REGISTRY.get(task_obj.task_name)
            if fn is None:
                self.queue.mark_failed(task_obj.id, f"no task registered as '{task_obj.task_name}'")
                continue

            try:
                result = fn(*task_obj.args, **task_obj.kwargs)
                self.queue.mark_done(task_obj.id, result)
            except Exception:
                self.queue.mark_failed(task_obj.id, traceback.format_exc())
