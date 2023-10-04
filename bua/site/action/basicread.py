import traceback
from typing import Dict, Callable
from pymysql import IntegrityError
from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action.accounts import Accounts
from bua.site.action.control import Control
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class BasicRead(Accounts):
    def __init__(self, queue: Queue, conn: DB, log: Callable, debug: bool, batch_size=100):
        Accounts.__init__(self, queue, conn, log, debug, batch_size)

    def initiate_basic_read_calculation(self, run_type: str, today: str, run_date: str, identifier_type: str):
        self._reset_control_records(run_type, today, run_date, identifier_type)
        self.queue_eligible_accounts(run_type, today, run_date, identifier_type)

    def initiate_reset_basic_read_calculation(self, run_type: str, today: str, run_date: str, identifier_type: str):
        self._reset_control_records('BasicRead', today, run_date, identifier_type)
        self.queue_eligible_accounts(run_type, today, run_date, identifier_type)

    def _reset_control_records(self, run_type: str, today: str, run_date: str, identifier_type: str):
        with self.conn.cursor() as cur:
            control = Control(run_type, today, today, today, run_date, identifier_type)
            control.reset_control_records(cur)
            self.conn.commit()

    def execute_basic_read_calculation(
            self, run_type: str, today: str, run_date: str, identifier_type: str, account_id: int
    ) -> Dict:
        control = Control(run_type, today, today, today, run_date, identifier_type)
        with self.conn.cursor() as cur:
            try:
                self.log(f'Executing {run_type} for account {account_id}')
                sql = """
                CALL bua_create_basic_read(JSON_OBJECT(
                    'in_accrual_end_date', DATE_SUB(%s, INTERVAL 1 DAY),
                    'in_debug', 0,
                    'in_account_id', %s,
                    'in_invoice_id', 0,
                    'in_tolerance_days', 1,
                    'in_run_date', %s,
                    'in_utility_profile_type', %s 
                ))"""
                cur.execute(sql, (today, account_id, run_date, identifier_type))
                cur.fetchall()
                self.conn.commit()
                control.insert_control_record(self.conn, cur, str(account_id), STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
            except IntegrityError as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.insert_control_record(
                    self.conn, cur, str(account_id), STATUS_FAIL, reason='IntegrityError', extra=str(ex)
                )
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex)
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def execute_reset_basic_read_calculation(
            self, run_type: str, today: str, run_date: str, identifier_type: str, account_id: int
    ) -> Dict:
        control = Control(run_type, today, today, today, run_date, identifier_type)
        with self.conn.cursor() as cur:
            try:
                self.log(f'Executing {run_type} for account {account_id}')
                sql = """
                CALL bua_reset_basic_read(%s,0)"""
                cur.execute(sql, (account_id,))
                cur.fetchall()
                self.conn.commit()
                control.insert_control_record(self.conn, cur, str(account_id), STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
            except IntegrityError as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.insert_control_record(
                    self.conn, cur, str(account_id), STATUS_FAIL, reason='IntegrityError', extra=str(ex)
                )
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex)
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
