import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    task_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at REAL NOT NULL,
    available_at REAL NOT NULL,
    result TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_claim
    ON tasks (status, available_at, priority, created_at);
"""


class ConnectionPool:
    """One sqlite3 connection per thread. sqlite3 connections aren't safe to
    share across threads, and a fresh connection per thread is cheap enough
    that pooling complexity isn't worth it here.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self._init_schema()

    def _init_schema(self):
        conn = sqlite3.connect(self.db_path)
        conn.executescript(SCHEMA)
        conn.commit()
        conn.close()

    def get(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn
