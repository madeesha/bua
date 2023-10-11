import traceback
from typing import Callable
from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action import Action


class Accounts(Action):

    def __init__(self, queue: Queue, conn: DB, log: Callable, debug: bool, batch_size=100):
        Action.__init__(self, queue, conn, log, debug)
        self.batch_size = batch_size

    def queue_eligible_accounts(
            self,
            run_type: str,
            today: str,
            run_date: str,
            identifier_type: str,
            all_accounts=False
    ):
        with self.conn.cursor() as cur:
            try:
                if all_accounts:
                    sql = "CALL bua_list_all_accounts(%s,%s,%s,%s)"
                else:
                    sql = "CALL bua_list_unbilled_accounts(%s,%s,%s,%s)"
                params = (None, None, today, run_date)
                cur.execute(sql, params)

                total = 0
                bodies = []
                body = None
                for record in cur.fetchall_unbuffered():
                    account_id = record['account_id']
                    if body is not None:
                        self.queue.send_if_needed(bodies, force=False, batch_size=self.batch_size)
                    body = {
                        'account_id': account_id,
                        'run_type': run_type,
                        'today': today,
                        'run_date': run_date,
                        'identifier_type': identifier_type
                    }
                    bodies.append(body)
                    total += 1
                self.queue.send_if_needed(bodies, force=True, batch_size=self.batch_size)

                self.log(f'{total} accounts to generate {run_type} data')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
