import argparse
import importlib
import json
import sys

from .queue import TaskQueue
from .worker import Worker


def main():
    parser = argparse.ArgumentParser(prog="taskqueue")
    parser.add_argument("--db", default="taskqueue.db", help="Path to the SQLite queue file")
    sub = parser.add_subparsers(dest="command", required=True)

    enqueue_p = sub.add_parser("enqueue", help="Enqueue a task")
    enqueue_p.add_argument("task_name")
    enqueue_p.add_argument("--args", default="[]", help="JSON array of positional args")
    enqueue_p.add_argument("--kwargs", default="{}", help="JSON object of keyword args")
    enqueue_p.add_argument("--priority", type=int, default=0)
    enqueue_p.add_argument("--max-retries", type=int, default=3)

    worker_p = sub.add_parser("worker", help="Run a worker pool")
    worker_p.add_argument("--import", dest="import_module", required=True,
                           help="Python module to import so @task-decorated functions register")
    worker_p.add_argument("--concurrency", type=int, default=4)

    status_p = sub.add_parser("status", help="Look up a task by id")
    status_p.add_argument("task_id")

    sub.add_parser("stats", help="Show task counts by status")

    args = parser.parse_args()
    queue = TaskQueue(args.db)

    if args.command == "enqueue":
        task_id = queue.enqueue(
            args.task_name,
            args=json.loads(args.args),
            kwargs=json.loads(args.kwargs),
            priority=args.priority,
            max_retries=args.max_retries,
        )
        print(task_id)

    elif args.command == "worker":
        importlib.import_module(args.import_module)
        worker = Worker(queue, concurrency=args.concurrency)
        try:
            worker.run()
        except KeyboardInterrupt:
            worker.stop()

    elif args.command == "status":
        t = queue.get(args.task_id)
        if t is None:
            print(f"no such task: {args.task_id}", file=sys.stderr)
            sys.exit(1)
        print(json.dumps({
            "id": t.id, "task_name": t.task_name, "status": t.status,
            "attempts": t.attempts, "result": t.result, "error": t.error,
        }, indent=2))

    elif args.command == "stats":
        print(json.dumps(queue.stats(), indent=2))


if __name__ == "__main__":
    main()
