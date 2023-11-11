import traceback

from pymysql import Connection


class Database(Connection):
    def __init__(self, rowcount=0):
        Connection.__init__(self, defer_connect=True)
        self.executions = []
        self.commits = []
        self.unbuffered_result = []
        self.rowcount = rowcount

    def cursor(self, cursor=None):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def execute(self, sql, args=None):
        self.executions.append((sql, args))
        try:
            if args is not None:
                sql % tuple(args)
        except TypeError as te:
            traceback.print_exception(te)
            print(sql)
            print(args)
            sql_arg_count = (len(sql) - len(sql.replace('%s', ''))) // 2
            print('Required', sql_arg_count, 'args but received ', len(args) if args is not None else 0, 'arguments')
            raise

    def commit(self):
        self.commits.append(True)

    def fetchall_unbuffered(self):
        return self.unbuffered_result


class Printer:
    def __init__(self):
        self.prints = []

    def print(self, *args, **kwargs):
        formatted = ' '.join([str(v) for v in args])
        self.prints.append((formatted, args, kwargs))
        print(*args, **kwargs)


class MySQL:
    def __init__(self, db: Database):
        self._db = db
        self.cursors = self
        self.SSDictCursor = self

    def connect(self, **_kwargs):
        return self._db
