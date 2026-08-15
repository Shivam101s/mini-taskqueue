import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from .db import ConnectionPool


@dataclass
class Task:
    id: str
    task_name: str
    args: list
    kwargs: dict
    priority: int
    status: str
    attempts: int
    max_retries: int
    result: Optional[str]
    error: Optional[str]

    @classmethod
    def from_row(cls, row) -> "Task":
        payload = json.loads(row["payload"])
        return cls(
            id=row["id"],
            task_name=row["task_name"],
            args=payload["args"],
            kwargs=payload["kwargs"],
            priority=row["priority"],
            status=row["status"],
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            result=row["result"],
            error=row["error"],
        )


class TaskQueue:
    """A durable, priority-ordered task queue backed by SQLite.

    Safe for multiple threads/processes to enqueue and claim tasks from
    concurrently: claiming uses a BEGIN IMMEDIATE transaction, which takes a
    write lock before reading, so two workers can never claim the same task.
    """

    def __init__(self, db_path: str = "taskqueue.db"):
        self.pool = ConnectionPool(db_path)

    def enqueue(
        self,
        task_name: str,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        priority: int = 0,
        max_retries: int = 3,
        delay: float = 0.0,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = time.time()
        payload = json.dumps({"args": args or [], "kwargs": kwargs or {}})
        conn = self.pool.get()
        conn.execute(
            """INSERT INTO tasks
               (id, task_name, payload, priority, status, attempts, max_retries,
                created_at, available_at)
               VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)""",
            (task_id, task_name, payload, priority, max_retries, now, now + delay),
        )
        conn.commit()
        return task_id

    def claim_next(self) -> Optional[Task]:
        conn = self.pool.get()
        now = time.time()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """SELECT * FROM tasks
                   WHERE status = 'pending' AND available_at <= ?
                   ORDER BY priority DESC, created_at ASC
                   LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                conn.rollback()
                return None
            conn.execute(
                "UPDATE tasks SET status = 'running', attempts = attempts + 1 WHERE id = ?",
                (row["id"],),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (row["id"],)).fetchone()
        return Task.from_row(row)

    def mark_done(self, task_id: str, result: Any = None):
        conn = self.pool.get()
        conn.execute(
            "UPDATE tasks SET status = 'done', result = ? WHERE id = ?",
            (json.dumps(result), task_id),
        )
        conn.commit()

    def mark_failed(self, task_id: str, error: str, backoff_base: float = 2.0):
        conn = self.pool.get()
        row = conn.execute(
            "SELECT attempts, max_retries FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if row["attempts"] < row["max_retries"]:
            delay = backoff_base ** row["attempts"]
            conn.execute(
                """UPDATE tasks SET status = 'pending', available_at = ?, error = ?
                   WHERE id = ?""",
                (time.time() + delay, error, task_id),
            )
        else:
            conn.execute(
                "UPDATE tasks SET status = 'failed', error = ? WHERE id = ?",
                (error, task_id),
            )
        conn.commit()

    def get(self, task_id: str) -> Optional[Task]:
        conn = self.pool.get()
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return Task.from_row(row) if row else None

    def stats(self) -> dict:
        conn = self.pool.get()
        rows = conn.execute(
            "SELECT status, COUNT(*) as count FROM tasks GROUP BY status"
        ).fetchall()
        return {row["status"]: row["count"] for row in rows}
