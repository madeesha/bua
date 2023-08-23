import traceback
from typing import Optional

from pymysql import Connection

from bua.site.action import Action


class NEM12(Action):

    def __init__(self, queue, conn: Connection, debug=False, batch_size=100):
        super().__init__(queue, conn, debug)
        self.batch_size = batch_size

    def initiate_nem12_file_generation(
            self,
            run_type: str,
            today: str,
            run_date: str,
            start_inclusive: Optional[str],
            end_exclusive: Optional[str],
            identifier_type: str
    ):
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "CALL bua_list_profile_nmis(%s, %s, %s, %s)",
                    (start_inclusive, end_exclusive, today, run_date)
                )
                total = 0
                bodies = []
                body = None
                for record in cur.fetchall_unbuffered():
                    nmi = record['nmi']
                    start_date = record['start_inclusive']
                    end_date = record['end_exclusive']
                    if body is not None:
                        self.send_if_needed(bodies, force=False, batch_size=self.batch_size)
                    body = {
                        'run_type': run_type,
                        'today': today,
                        'run_date': run_date,
                        'nmi': nmi,
                        'start_inclusive': start_date,
                        'end_exclusive': end_date,
                        'identifier_type': identifier_type
                    }
                    bodies.append(body)
                    total += 1
                self.send_if_needed(bodies, force=True, batch_size=self.batch_size)
                self.log(f'{total} sites to generate {run_type} profiled estimates data')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def nem12_file_generation(self, run_type: str, nmi: str, start_inclusive: str, end_exclusive:str, today: str, run_date: str, identifier_type: str):
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "CALL bua_list_missing_periods(%s, %s, %s, %s, %s, %s)",
                    (nmi, start_inclusive, end_exclusive, today, run_date, identifier_type)
                )
                total = 0
                for record in cur.fetchall_unbuffered():
                    print(record)
                    total += 1
                self.log(f'{total} {run_type} profiled estimates for {nmi}')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
