import traceback
from typing import Optional

from bua.facade.connection import DB


class Control:
    def __init__(
            self, conn: DB, run_type: str,
            start_inclusive: str, end_exclusive: str,
            today: str, run_date: str, identifier_type: str
    ):
        self.conn = conn
        self.run_type = run_type
        self.start_inclusive = start_inclusive
        self.end_exclusive = end_exclusive
        self.today = today
        self.run_date = run_date
        self.identifier_type = identifier_type

    def reset_control_records(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM BUAControl WHERE run_type = %s AND run_date = %s",
                (self.run_type, self.run_date)
            )
            self.conn.commit()

    def insert_control_record(
            self, identifier: str, status: str,
            rows_counted: Optional[int] = None,
            rows_written: Optional[int] = None,
            reason: Optional[str] = None,
            extra: Optional[str] = None,
            key: Optional[str] = None
    ):
        with self.conn.cursor() as cur:
            try:
                if reason is not None:
                    reason = reason[0:255]
                if extra is not None:
                    extra = extra[0:255]
                if key is not None:
                    key = key[0:255]
                sql = """
                        REPLACE INTO BUAControl
                        ( run_type, identifier, start_inclusive, end_exclusive, today, run_date
                        , identifier_type, rows_counted, rows_written, status, reason, extra, s3_key)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                args = (self.run_type, identifier,
                        self.start_inclusive, self.end_exclusive,
                        self.today, self.run_date,
                        self.identifier_type, rows_counted, rows_written, status, reason,
                        extra, key)
                cur.execute(sql, args)
                self.conn.commit()
            except Exception as e2:
                traceback.print_exception(e2)
                self.conn.rollback()
