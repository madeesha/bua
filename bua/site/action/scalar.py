import traceback
from typing import Callable
from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action.accounts import Accounts
from bua.site.handler import STATUS_DONE


class MicroScalar(Accounts):
    def __init__(self, queue: Queue, conn: DB, log: Callable, debug: bool, batch_size=100):
        Accounts.__init__(self, queue, conn, log, debug, batch_size)

    def initiate_microscalar_calculation(
            self, run_type: str, today: str, run_date: str, identifier_type: str, end_inclusive: str
    ):
        self.queue_eligible_accounts(run_type, today, run_date, identifier_type, end_inclusive, all_accounts=False)

    def execute_microscalar_calculation(self, run_type, today, run_date, identifier_type, account_id):
        with self.conn.cursor() as cur:
            try:
                self.log(f'Executing {run_type} for account {account_id}')
                sql = """
                CALL bua_create_invoice_scalar(JSON_OBJECT(
                    'in_accrual_end_date', DATE_SUB(%s, INTERVAL 1 DAY),
                    'in_debug', 0,
                    'in_account_id', %s,
                    'in_invoice_id', -1,
                    'in_tolerance_days', 1,
                    'in_run_date', %s,
                    'in_utility_profile_type', %s 
                ))"""
                cur.execute(sql, (today, account_id, run_date, identifier_type))
                cur.fetchall()
                self.conn.commit()
                return {
                    'status': STATUS_DONE
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
