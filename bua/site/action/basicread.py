import traceback
from typing import Dict, Callable
from pymysql import DatabaseError, InternalError, InterfaceError
from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action.accounts import Accounts
from bua.site.action.control import Control
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class BasicRead(Accounts):
    def __init__(self, queue: Queue, conn: DB, ctl_conn: DB, log: Callable, debug: bool, batch_size=100):
        Accounts.__init__(self, queue, conn, ctl_conn, log, debug, batch_size)

    def initiate_basic_read_calculation(self, run_type: str, today: str, run_date: str, identifier_type: str,
                                        start_inclusive: str, end_exclusive: str, end_inclusive: str,
                                        proc_name=None):
        self.reset_control_records(run_type, today, run_date, identifier_type)
        self.queue_eligible_accounts(run_type, today, run_date, identifier_type,
                                     start_inclusive, end_exclusive, end_inclusive,
                                     all_accounts=False, proc_name=proc_name)

    def initiate_reset_basic_read_calculation(self, run_type: str, today: str, run_date: str, identifier_type: str,
                                              start_inclusive: str, end_exclusive: str, end_inclusive: str,
                                              proc_name=None):
        self.reset_control_records(run_type, today, run_date, identifier_type)
        self.queue_eligible_accounts(run_type, today, run_date, identifier_type,
                                     start_inclusive, end_exclusive, end_inclusive,
                                     all_accounts=False, proc_name=proc_name)

    def execute_basic_read_calculation(
            self, run_type: str, today: str, run_date: str, identifier_type: str,
            start_inclusive: str, end_exclusive: str, account_id: int
    ) -> Dict:
        control = Control(self.ctl_conn, run_type, start_inclusive, end_exclusive, today, run_date, identifier_type)
        with self.conn.cursor() as cur:
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
            params = (today, account_id, run_date, identifier_type)
            try:
                self.log(f'Executing {run_type} for account {account_id}')
                cur.execute(sql, params)
                cur.fetchall()
                self.conn.commit()
                control.update_control_record(str(account_id), STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except DatabaseError as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.update_control_record(str(account_id), STATUS_FAIL, reason='DatabaseError', extra=str(ex))
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex),
                    'context': {
                        'sql': sql,
                        'params': list(params),
                    }
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.update_control_record(str(account_id), STATUS_FAIL, reason='UnhandledError', extra=str(ex))
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex),
                    'context': {
                        'sql': sql,
                        'params': list(params),
                    }
                }

    def execute_reset_basic_read_calculation(
            self, run_type: str, today: str, run_date: str, identifier_type: str,
            start_inclusive: str, end_exclusive: str, account_id: int
    ) -> Dict:
        control = Control(self.ctl_conn, run_type, start_inclusive, end_exclusive, today, run_date, identifier_type)
        with self.conn.cursor() as cur:
            sql = """
            CALL bua_reset_basic_read(%s,0)
            """
            params = (account_id,)
            try:
                self.log(f'Executing {run_type} for account {account_id}')
                cur.execute(sql, params)
                cur.fetchall()
                self.conn.commit()
                control.update_control_record(str(account_id), STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except DatabaseError as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.update_control_record(str(account_id), STATUS_FAIL, reason='DatabaseError', extra=str(ex))
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex),
                    'context': {
                        'sql': sql,
                        'params': list(params),
                    }
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.update_control_record(str(account_id), STATUS_FAIL, reason='UnhandledError', extra=str(ex))
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex),
                    'context': {
                        'sql': sql,
                        'params': list(params),
                    }
                }
