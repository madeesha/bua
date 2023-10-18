import traceback
from typing import Callable
from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action import Action
from bua.site.action.control import Control


class Accounts(Action):

    def __init__(self, queue: Queue, conn: DB, ctl_conn: DB, log: Callable, debug: bool, batch_size=100):
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
            all_accounts=False,
            account_limit=-1,
            proc_name=None
    ):
        self._prepare_accounts_to_process(all_accounts, identifier_type, run_date, run_type, today,
                                          start_inclusive, end_exclusive,
                                          account_limit=account_limit, proc_name=proc_name)
        self._queue_accounts_to_process(end_exclusive, end_inclusive, identifier_type, run_date, run_type,
                                        start_inclusive, today)

    def _queue_accounts_to_process(self, end_exclusive, end_inclusive, identifier_type, run_date, run_type,
                                   start_inclusive, today):
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
                    'end_inclusive': end_inclusive
                }
                bodies.append(body)
                total += 1
            self.queue.send_if_needed(bodies, force=True, batch_size=self.batch_size)
            self.log(f'{total} accounts to generate {run_type} data')

    def _prepare_accounts_to_process(
            self, all_accounts, identifier_type, run_date, run_type, today, start_inclusive, end_exclusive,
            account_limit=-1, proc_name=None
    ):
        control = Control(self.ctl_conn, run_type, start_inclusive, end_exclusive, today, run_date, identifier_type)
        with self.conn.cursor() as cur:
            try:
                if proc_name is None:
                    if all_accounts:
                        proc_name = "bua_list_all_accounts"
                    else:
                        proc_name = "bua_list_unbilled_accounts"
                sql = f"CALL {proc_name}(%s,%s,%s,%s)"
                params = (None, None, today, run_date)
                cur.execute(sql, params)
                total = 0
                for record in cur.fetchall_unbuffered():
                    account_id = record['account_id']
                    control.insert_control_record(str(account_id), 'PREP', commit=False)
                    total += 1
                    if 0 < account_limit <= total:
                        break
                self.conn.commit()
                control.conn.commit()
                self.log(f'{total} accounts prepared for {run_type} data')
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
