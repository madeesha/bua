import traceback
from typing import Callable, Dict

from pymysql import InternalError, InterfaceError

from bua.facade.connection import DBProxy
from bua.facade.sqs import Queue
from bua.site.action import Action
from bua.site.action.control import Control


class Accounts(Action):

    def __init__(self, queue: Queue, conn: DBProxy, ctl_conn: DBProxy, log: Callable, debug: bool, batch_size=100):
        Action.__init__(self, queue, conn, log, debug)
        self.ctl_conn = ctl_conn
        self.batch_size = batch_size

    def reset_control_records(self, run_type: str, today: str, run_date: str, identifier_type: str):
        control = Control(self.ctl_conn, run_type, today, today, today, run_date, identifier_type)
        control.reset_control_records()

    def queue_eligible_accounts(
            self,
            run_type: str,
            today: str,
            run_date: str,
            identifier_type: str,
            start_inclusive: str,
            end_exclusive: str,
            end_inclusive: str,
            db: Dict[str, str],
            all_accounts=False,
            proc_name=None
    ):
        self._prepare_accounts_to_process(all_accounts, identifier_type, run_date, run_type, today,
                                          start_inclusive, end_exclusive,
                                          proc_name=proc_name)
        self._queue_accounts_to_process(end_exclusive, end_inclusive, identifier_type, run_date, run_type,
                                        start_inclusive, today, db)

    def _queue_accounts_to_process(self, end_exclusive, end_inclusive, identifier_type, run_date, run_type,
                                   start_inclusive, today, db):
        with self.conn.cursor() as cur:
            stmt = "SELECT identifier FROM BUAControl WHERE run_type = %s AND run_date = %s AND status = 'PREP'"
            params = (run_type, run_date)
            cur.execute(stmt, params)
            total = 0
            bodies = []
            body = None
            for record in cur.fetchall_unbuffered():
                account_id = int(record['identifier'])
                if body is not None:
                    self.queue.send_if_needed(bodies, force=False, batch_size=self.batch_size)
                body = {
                    'account_id': account_id,
                    'run_type': run_type,
                    'today': today,
                    'run_date': run_date,
                    'identifier_type': identifier_type,
                    'start_inclusive': start_inclusive,
                    'end_exclusive': end_exclusive,
                    'end_inclusive': end_inclusive,
                    'db': db,
                }
                bodies.append(body)
                total += 1
            self.queue.send_if_needed(bodies, force=True, batch_size=self.batch_size)
            self.log(f'{total} accounts to generate {run_type} data')

    def _prepare_accounts_to_process(
            self, all_accounts, identifier_type, run_date, run_type, today, start_inclusive, end_exclusive,
            proc_name=None
    ):
        with self.conn.cursor() as cur:
            try:
                if proc_name is None:
                    if all_accounts:
                        proc_name = "bua_prep_all_accounts"
                    else:
                        proc_name = "bua_prep_unbilled_accounts"
                sql = f"CALL {proc_name}(%s,%s,%s,%s,%s,%s)"
                params = (start_inclusive, end_exclusive, today, run_date, run_type, identifier_type)
                total = cur.execute(sql, params)
                self.conn.commit()
                self.log(f'{total} accounts prepared for {run_type} data')
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
