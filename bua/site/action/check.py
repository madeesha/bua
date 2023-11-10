import traceback
from typing import Callable

from pymysql import IntegrityError, InternalError, InterfaceError

from bua.facade.connection import DBProxy
from bua.facade.sqs import Queue
from bua.site.action.datestocheck import DatesToCheck
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class Check(DatesToCheck):

    def __init__(self, queue: Queue, conn: DBProxy, log: Callable, debug: bool, batch_size=1):
        DatesToCheck.__init__(self, queue, conn, log, debug, batch_size)

    def initiate_segment_jurisdiction_check(
            self, run_type, run_date, today, start_inclusive, end_exclusive, identifier_type, db
    ):
        self._initiate_dates_to_check(run_type, run_date, today, start_inclusive, end_exclusive, identifier_type, db)

    def segment_jurisdiction_check(self, run_date, identifier_type, interval_date):
        with self.conn.cursor() as cur:
            try:
                self.log(f'Jurisdiction check {identifier_type} on {interval_date} for run date {run_date}')
                cur.execute(
                    "CALL bua_mark_segment_jurisdiction_entries(%s, %s, %s)",
                    (run_date, identifier_type, interval_date)
                )
                for record in cur.fetchall():
                    count = record['total_invalid_entries']
                    self.log(f'{count} invalid entries')
                self.conn.commit()
                return {
                    'status': STATUS_DONE
                }
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except IntegrityError as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex)
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
