import traceback
from typing import Dict

from pymysql import Connection, IntegrityError

from bua.site.action import Action
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class BasicRead(Action):
    def __init__(self, queue, conn: Connection, debug=False, batch_size=100):
        super().__init__(queue, conn, debug)
        self.batch_size = batch_size

    def initiate_basic_read_calculation(
        self,
        run_type: str,
        today: str,
        run_date: str,
        identifier_type: str
    ):
        with self.conn.cursor() as cur:
            try:

                sql = """
                SELECT DISTINCT ac.id
                FROM Account ac
                JOIN AccountBilling ab ON ac.id = ab.account_id
                WHERE ac.commence_date <= COALESCE(ac.closed_date, %s)
                AND COALESCE(ac.closed_date, %s) >= DATE_SUB(%s, INTERVAL 1 YEAR)"""

                cur.execute(sql, (today, today, today))

                total = 0
                bodies = []
                body = None
                for record in cur.fetchall_unbuffered():
                    account_id = record['id']
                    if body is not None:
                        self.send_if_needed(bodies, force=False, batch_size=self.batch_size)
                    body = {
                        'account_id': account_id,
                        'run_type': run_type,
                        'today': today,
                        'run_date': run_date,
                        'identifier_type': identifier_type
                    }
                    bodies.append(body)
                    total += 1
                self.send_if_needed(bodies, force=True, batch_size=self.batch_size)

                self.log(f'{total} accounts to generate {run_type} data')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def execute_basic_read_calculation(
            self, run_type: str, today: str, run_date: str, identifier_type: str, account_id: int
    ) -> Dict:
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
                return {
                    'status': STATUS_DONE
                }
            except IntegrityError as ex:
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex)
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
