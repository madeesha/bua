from typing import Dict, List


class MySQLStub:

    def __init__(self, resultsets: List[Dict], affectedrows: List[int]):
        self.resultsets = resultsets
        self.affectedrows = affectedrows

    def connect(self, host, user, password, database, cursorclass, autocommit):
        return self

    def cursor(self):
        return self

    def execute(self, *args):
        assert 1 <= len(args) <= 2, f'Unexpected number of arguments to execute {len(args)}'

    def commit(self):
        pass

    def fetchall(self):
        return self.resultsets.pop(0)

    def affected_rows(self):
        return self.affectedrows.pop(0)

    def mogrify(self, query, args=None):
        return query % self._escape(args)

    @staticmethod
    def _escape(args):
        return tuple(
            f"'{arg}'" if isinstance(arg, str) else arg
            for arg in args
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return
