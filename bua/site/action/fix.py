import traceback
from typing import Callable
from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action.datestocheck import DatesToCheck
from bua.site.handler import STATUS_DONE


class Fix(DatesToCheck):

    def __init__(self, queue: Queue, conn: DB, log: Callable, debug: bool, batch_size=1):
        DatesToCheck.__init__(self, queue, conn, log, debug, batch_size)

    def initiate_segment_jurisdiction_fix(
        self, run_type, run_date, today, start_inclusive, end_exclusive, identifier_type
    ):
        self._initiate_dates_to_check(run_type, run_date, today, start_inclusive, end_exclusive, identifier_type)

    def segment_jurisdiction_fix(self, run_date, identifier_type, interval_date):
        with self.conn.cursor() as cur:
            try:
                self.log(f'Jurisdiction fix {identifier_type} on {interval_date} for run date {run_date}')
                cur.execute(
                    "CALL bua_fill_marked_segment_jurisdiction_entries(%s, %s, %s)",
                    (run_date, identifier_type, interval_date)
                )
                for record in cur.fetchall():
                    count = record['total_invalid_entries']
                    self.log(f'{count} invalid entries')
                self.conn.commit()
                return {
                    'status': STATUS_DONE
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
