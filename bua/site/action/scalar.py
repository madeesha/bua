import traceback

from pymysql import Connection

from bua.site.action import Action


class MicroScalar(Action):
    def __init__(self, queue, conn: Connection, debug=False, batch_size=100):
        super().__init__(queue, conn, debug)
        self.batch_size = batch_size

    def initiate_microscalar_calculation(
        self,
        run_type: str,
        today: str,
        run_date: str,
        identifier_type: str
    ):
        with self.conn.cursor() as cur:
            try:

                cur.execute("TRUNCATE TABLE InvoiceScalar")

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
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
