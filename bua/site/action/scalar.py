import traceback
from typing import Callable

from pymysql import IntegrityError

from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action.accounts import Accounts
from bua.site.action.control import Control
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class MicroScalar(Accounts):
    def __init__(self, queue: Queue, conn: DB, ctl_conn: DB, log: Callable, debug: bool, batch_size=100):
        Accounts.__init__(self, queue, conn, ctl_conn, log, debug, batch_size)

    def initiate_microscalar_calculation(
            self, run_type: str, today: str, run_date: str, identifier_type: str,
            start_inclusive: str, end_exclusive: str, end_inclusive: str
    ):
        self.reset_control_records(run_type, today, run_date, identifier_type)
        self.queue_eligible_accounts(
            run_type, today, run_date, identifier_type,
            start_inclusive, end_exclusive, end_inclusive, all_accounts=False
        )

    def execute_microscalar_calculation(
            self, run_type, today, run_date, identifier_type, start_inclusive, end_exclusive, account_id
    ):
        control = Control(self.ctl_conn, run_type, start_inclusive, end_exclusive, today, run_date, identifier_type)
        with self.conn.cursor() as cur:
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
            params = [today, account_id, run_date, identifier_type]
            try:
                self.log(f'Executing {run_type} for account {account_id}')
                cur.execute(sql, params)
                cur.fetchall()
                self.conn.commit()
                control.insert_control_record(f'{account_id}', STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
            except IntegrityError as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.insert_control_record(f'{account_id}', STATUS_FAIL, reason='IntegrityError', extra=str(ex))
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex),
                    'context': {
                        'sql': sql,
                        'params': params
                    }
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
