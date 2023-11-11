import traceback
from typing import Callable

from pymysql import InternalError, InterfaceError

from bua.facade.connection import DBProxy
from bua.facade.sqs import Queue
from bua.site.action import Action


class DatesToCheck(Action):

    def __init__(self, queue: Queue, conn: DBProxy, log: Callable, debug: bool, batch_size: int):
        Action.__init__(self, queue, conn, log, debug)
        self.batch_size = batch_size

    def _initiate_dates_to_check(
            self, run_type, run_date, today, start_inclusive, end_exclusive, identifier_type, db
    ):
        with self.conn.cursor() as cur:
            try:
                bodies = []
                total = 0
                cur.execute(
                    "CALL bua_dates_to_check(%s, %s, %s, %s)",
                    (run_date, today, start_inclusive, end_exclusive)
                )
                for record in cur.fetchall_unbuffered():
                    interval_date = record['interval_date']
                    body = {
                        'run_type': run_type,
                        'run_date': run_date,
                        'identifier_type': identifier_type,
                        'interval_date': interval_date.strftime('%Y-%m-%d'),
                    }
                    bodies.append(body)
                    total += 1
                    self.queue.send_if_needed(bodies, batch_size=self.batch_size, db=db)
                self.queue.send_if_needed(bodies, force=True, batch_size=self.batch_size, db=db)
                self.conn.commit()
                self.log(f'{total} dates to check')
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
