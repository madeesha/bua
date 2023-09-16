import traceback
from typing import Optional

from pymysql import Connection
from pymysql.cursors import Cursor


class Control:
    def __init__(
            self, run_type: str, identifier: str,
            start_inclusive: str, end_exclusive: str,
            today: str, run_date: str, identifier_type: str
    ):
        self.run_type = run_type
        self.identifier = identifier
        self.start_inclusive = start_inclusive
        self.end_exclusive = end_exclusive
        self.today = today
        self.run_date = run_date
        self.identifier_type = identifier_type

    def insert_control_record(
            self, conn: Connection, cur: Cursor, status: str,
            rows_counted: Optional[int] = None, rows_written: Optional[int] = None,
            reason: Optional[str] = None, extra: Optional[str] = None, key: Optional[str] = None
    ):
        try:
            if extra is not None:
                extra = extra[0:255]
            sql = """
                    INSERT INTO BUAControl
                    ( run_type, identifier, start_inclusive, end_exclusive, today, run_date
                    , identifier_type, rows_counted, rows_written, status, reason, extra, s3_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
            args = (self.run_type, self.identifier,
                    self.start_inclusive, self.end_exclusive,
                    self.today, self.run_date,
                    self.identifier_type, rows_counted, rows_written, status, reason,
                    extra, key)
            cur.execute(sql, args)
            conn.commit()
        except Exception as e2:
            traceback.print_exception(e2)
            conn.rollback()
