import traceback
from typing import Dict

from pymysql import Connection, IntegrityError

from bua.site.action import Action
from bua.site.action.accounts import Accounts
from bua.site.action.control import Control
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class BasicRead(Action, Accounts):
    def __init__(self, queue, conn: Connection, debug=False, batch_size=100):
        Action.__init__(self, queue, conn, debug)
        Accounts.__init__(self, queue, conn, batch_size, debug)

    def initiate_basic_read_calculation(self, run_type: str, today: str, run_date: str, identifier_type: str):
        self.queue_eligible_accounts(run_type, today, run_date, identifier_type)

    def execute_basic_read_calculation(
            self, run_type: str, today: str, run_date: str, identifier_type: str, account_id: int
    ) -> Dict:
        control = Control(run_type, str(account_id), today, today, today, run_date, identifier_type)
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
                control.insert_control_record(self.conn, cur, STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
            except IntegrityError as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.insert_control_record(self.conn, cur, STATUS_FAIL, reason='IntegrityError', extra=str(ex))
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex)
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
