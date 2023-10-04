from pymysql import Connection
from pymysql.cursors import SSDictCursor


class DB:

    def __init__(self, conn: Connection):
        self.conn = conn

    def cursor(self) -> SSDictCursor:
        return self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()
