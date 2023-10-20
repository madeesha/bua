import traceback
from datetime import date
from typing import Optional, Union

from bua.facade.connection import DB


class Control:
    def __init__(
            self, conn: DB, run_type: str,
            start_inclusive: str, end_exclusive: str,
            today: str, run_date: str, identifier_type: str
    ):
        self.conn = conn
        self.cur = self.conn.cursor()
        self.run_type = run_type
        self.start_inclusive = start_inclusive
        self.end_exclusive = end_exclusive
        self.today = today
        self.run_date = run_date
        self.identifier_type = identifier_type

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def reset_control_records(self):
        self.cur.execute(
            "DELETE FROM BUAControl WHERE run_type = %s AND run_date = %s",
            (self.run_type, self.run_date)
        )
        self.commit()

    def insert_control_record(
            self, identifier: str, status: str,
            rows_counted: Optional[int] = None,
            rows_written: Optional[int] = None,
            reason: Optional[str] = None,
            extra: Optional[str] = None,
            key: Optional[str] = None,
            start_inclusive: Optional[Union[str, date]] = None,
            end_exclusive: Optional[Union[str, date]] = None,
            commit=True
    ):
        try:
            if reason is not None:
                reason = reason[0:255]
            if extra is not None:
                extra = extra[0:255]
            if key is not None:
                key = key[0:255]
            if start_inclusive is None:
                start_inclusive = self.start_inclusive
            if end_exclusive is None:
                end_exclusive = self.end_exclusive
            sql = """
                        REPLACE INTO BUAControl
                        ( run_type, identifier, start_inclusive, end_exclusive, today, run_date
                        , identifier_type, rows_counted, rows_written, status, reason, extra, s3_key, cr_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """
            args = (self.run_type, identifier,
                    start_inclusive, end_exclusive,
                    self.today, self.run_date,
                    self.identifier_type, rows_counted, rows_written, status, reason,
                    extra, key)
            row_count = self.cur.execute(sql, args)
            if row_count == 0:
                msg = f'NO INSERT OF BUAControl FOR {self.run_type}, {identifier}, {self.run_date}, {start_inclusive}'
                print(msg)
            if commit:
                self.commit()
        except Exception as e2:
            traceback.print_exception(e2)
            msg = f'NO INSERT OF BUAControl FOR {self.run_type}, {identifier}, {self.run_date}, {start_inclusive}'
            print(msg)
            if commit:
                self.rollback()

    def update_control_record(
            self, identifier: str,
            status: str,
            rows_counted: Optional[int] = None,
            rows_written: Optional[int] = None,
            reason: Optional[str] = None,
            extra: Optional[str] = None,
            key: Optional[str] = None,
            start_inclusive: Optional[Union[str, date]] = None,
            commit=True
    ):
        try:
            if reason is not None:
                reason = reason[0:255]
            if extra is not None:
                extra = extra[0:255]
            if key is not None:
                key = key[0:255]
            if start_inclusive is None:
                start_inclusive = self.start_inclusive
            sql = """
                UPDATE BUAControl
                SET status = %s
                , rows_counted = %s
                , rows_written = %s
                , reason = %s
                , extra = %s
                , s3_key = %s
                , cr_date = NOW()
                WHERE run_type = %s
                AND identifier = %s
                AND run_date = %s
                AND start_inclusive = %s
                """
            args = (
                status, rows_counted, rows_written, reason, extra, key,
                self.run_type, identifier, self.run_date, start_inclusive
            )
            row_count = self.cur.execute(sql, args)
            if row_count == 0:
                params = {
                    'run_type': self.run_type,
                    'identifier': identifier,
                    'run_date': self.run_date,
                    'start_inclusive': start_inclusive
                }
                msg = f'NO UPDATE OF BUAControl FOR {params}'
                print(msg)
            if commit:
                self.commit()
        except Exception as e2:
            traceback.print_exception(e2)
            params = {
                'run_type': self.run_type,
                'identifier': identifier,
                'run_date': self.run_date,
                'start_inclusive': start_inclusive
            }
            msg = f'NO UPDATE OF BUAControl FOR {params}'
            print(msg)
            if commit:
                self.rollback()
