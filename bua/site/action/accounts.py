import traceback

from pymysql import Connection

from bua.site.action import SQSAction


class Accounts(SQSAction):

    def __init__(self, queue, conn: Connection, batch_size=100, debug=False):
        SQSAction.__init__(self, queue, debug)
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

                sql = "CALL bua_list_unbilled_accounts(%s,%s,%s,%s)"
                params = (None, None, today, run_date)
                cur.execute(sql, params)

                total = 0
                bodies = []
                body = None
                for record in cur.fetchall_unbuffered():
                    account_id = record['account_id']
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
