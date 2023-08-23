import traceback
from typing import Optional, List, Dict
from datetime import datetime, timedelta
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
                records: List[Dict] = list(cur.fetchall())
                total = 0
                lines = []
                file_date_time = datetime.strptime(run_date, '%Y-%m-%d').strftime('%Y%m%d%H%M')
                update_date_time = datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S')
                update_date_time = update_date_time - timedelta(days=36525)
                update_date_time = update_date_time.strftime('%Y%m%d%H%M%S')
                lines.append(['100', 'NEM12', file_date_time, 'BUA', 'BUA'])
                nmi_configuration = ''.join({row['suffix_id'] for row in records})
                for record in records:
                    register_id = record['register_id']
                    suffix_id = record['suffix_id']
                    serial = record['serial']
                    unit_of_measure = record['unit_of_measure']
                    lines.append(['200', nmi, nmi_configuration, register_id, suffix_id, suffix_id, serial, unit_of_measure, '30', ''])
                    read_date = datetime.strptime(record['read_date'], '%Y-%m-%d').strftime('%Y%m%d')
                    values = [record[f'value_{index:02}'] for index in range(1, 49)]
                    lines.append(['300', read_date, *values, 'AB', '', '', update_date_time, ''])
                    total += 1
                lines.append(['900'])
                for line in lines:
                    print(','.join(line))
                self.log(f'{total} {run_type} profiled estimates for {nmi}')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
