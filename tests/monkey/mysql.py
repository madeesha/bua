from typing import List, Dict

from pymysql import InternalError


class MonkeyPatchConnection:
    def __init__(self):
        self._cursor = MonkeyPatchCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        return

    def rollback(self):
        return

    def patch(self):
        self._cursor.patch()


class MonkeyPatchCursor:

    def __init__(self):
        self.execute_fails = False
        self.execute_fails_after_invocations = -1
        self._execute_invocations = []
        self._result_sets: List[List[Dict]] = []
        self._result_set = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def patch(self):
        self.execute_fails = False
        self.execute_fails_after_invocations = -1
        self._execute_invocations = []
        self._result_sets = []
        self._result_set = None

    def assert_n_execute_invocations(self, n=0):
        assert len(self._execute_invocations) == n, self._execute_invocations

    def execute(self, *args, **kwargs):
        self._result_set = self._result_sets[len(self._execute_invocations)] \
            if len(self._execute_invocations) < len(self._result_sets) else []
        self._execute_invocations.append((args, kwargs))
        if self.execute_fails:
            raise InternalError('Database connection lost')
        if self.execute_fails_after_invocations > -1:
            if self.execute_fails_after_invocations < len(self._execute_invocations):
                raise InternalError('Database connection lost')
        return

    def fetchall(self):
        if self._result_set is None:
            raise InternalError('fetchall called before execute')
        return self._result_set

    def add_result_set(self, result_set: List[Dict]):
        self._result_sets.append(result_set)
