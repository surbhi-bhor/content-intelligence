import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The ops import as `ops.<name>` from dagster/, and the Flask agent as
# `agent` from flask/, matching how each container runs them.
sys.path.insert(0, os.path.join(ROOT, "dagster"))
sys.path.insert(0, os.path.join(ROOT, "flask"))

# Read at import time by some modules; tests never open a real connection.
os.environ.setdefault("ASK_DATABASE_URL", "postgresql://test:test@localhost:5432/test")


class FakeCursor:
    """Records every SQL statement and returns queued results, so ops can be
    exercised without a database."""

    def __init__(self, fetchone_results=None, fetchall_results=None):
        self.executed = []
        self._fetchone = list(fetchone_results or [])
        self._fetchall = list(fetchall_results or [])
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.executed.append(" ".join(sql.split()))

    def fetchone(self):
        return self._fetchone.pop(0) if self._fetchone else None

    def fetchall(self):
        return self._fetchall.pop(0) if self._fetchall else []

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        pass
