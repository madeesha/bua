import traceback

from pymysql import Connection

from bua.site.action import SQS


class Accounts(SQS):

    def __init__(self, queue, conn: Connection, batch_size=100, debug=False):
        SQS.__init__(self, queue, debug)
        self.conn = conn
        self.batch_size = batch_size

    def queue_eligible_accounts(
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
