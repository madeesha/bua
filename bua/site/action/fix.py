import traceback

from pymysql import Connection

from bua.site.action import Action
from bua.site.handler import STATUS_DONE


class Fix(Action):

    def __init__(self, queue, conn: Connection, debug=False, batch_size=1):
        super().__init__(queue=queue, conn=conn, debug=debug)
        self.batch_size = batch_size

    def initiate_segment_jurisdiction_fix(
            self, run_type, run_date, today, start_inclusive, end_exclusive, identifier_type
    ):
        with self.conn.cursor() as cur:
            try:
                bodies = []
                total = 0
                cur.execute(
                    "CALL bua_dates_to_check(%s, %s, %s, %s)",
                    (run_date, today, start_inclusive, end_exclusive)
                )
                for record in cur.fetchall_unbuffered():
                    interval_date = record['interval_date']
                    body = {
                        'run_type': run_type,
                        'run_date': run_date,
                        'identifier_type': identifier_type,
                        'interval_date': interval_date.strftime('%Y-%m-%d'),
                    }
                    bodies.append(body)
                    total += 1
                    self.send_if_needed(bodies, batch_size=self.batch_size)
                self.send_if_needed(bodies, force=True, batch_size=self.batch_size)
                self.conn.commit()
                self.log(f'{total} dates to check')
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def segment_jurisdiction_fix(self, run_date, identifier_type, interval_date):
        with self.conn.cursor() as cur:
            try:
                self.log(f'Jurisdiction fix {identifier_type} on {interval_date} for run date {run_date}')
                cur.execute(
                    "CALL bua_fill_marked_segment_jurisdiction_entries(%s, %s, %s)",
                    (run_date, identifier_type, interval_date)
                )
                for record in cur.fetchall():
                    count = record['total_invalid_entries']
                    self.log(f'{count} invalid entries')
                self.conn.commit()
                return {
                    'status': STATUS_DONE
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
