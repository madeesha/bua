import base64
import io
import traceback
from hashlib import md5
from typing import Optional, List, Dict
from datetime import datetime, timedelta

from pymysql import Connection
import csv
from bua.site.action import Action


class NEM12(Action):

    def __init__(self, queue, conn: Connection, debug=False, batch_size=100, s3_client=None, bucket_name=None):
        super().__init__(queue, conn, debug)
        self.batch_size = batch_size
        self.s3_client = s3_client
        self.bucket_name = bucket_name

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
                    if start_date <= end_date:
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

    def nem12_file_generation(self, run_type: str, nmi: str, start_inclusive: Optional[str], end_exclusive: Optional[str], today: str, run_date: str, identifier_type: str):
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "CALL bua_list_missing_periods(%s, %s, %s, %s, %s, %s)",
                    (nmi, start_inclusive, end_exclusive, today, run_date, identifier_type)
                )
                records: List[Dict] = list(cur.fetchall())
                total = 0
                file_date_time = datetime.strptime(run_date, '%Y-%m-%d').strftime('%Y%m%d%H%M')
                update_date_time = datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S')
                update_date_time = update_date_time - timedelta(days=36525)
                update_date_time = update_date_time.strftime('%Y%m%d%H%M%S')
                unique_id = f'{nmi}{update_date_time}'
                file_name = f'nem12#{unique_id}#bua#bua.csv'
                output = io.StringIO()
                writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(['100', 'NEM12', file_date_time, 'BUA', 'BUA'])
                nmi_configuration = ''.join({row['suffix_id'] for row in records})
                for record in records:
                    register_id = record['register_id']
                    suffix_id = record['suffix_id']
                    serial = record['serial']
                    unit_of_measure = record['unit_of_measure']
                    writer.writerow(['200', nmi, nmi_configuration, register_id, suffix_id, suffix_id, serial, unit_of_measure, '30', ''])
                    read_date = record['read_date'].strftime('%Y%m%d')
                    values = [record[f'value_{index:02}'] for index in range(1, 49)]
                    writer.writerow(['300', read_date, *values, 'AB', '', '', update_date_time, ''])
                    total += 1
                writer.writerow(['900'])
                body = output.getvalue().encode('utf-8')
                md5sum = base64.b64encode(md5(body).digest()).decode('utf-8')
                key = f'bua/{run_date}/{file_name}'
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    ContentMD5=md5sum,
                    ContentType='text/plain',
                    Body=body,
                    ContentLength=len(body)
                )
                self.log(f'{total} {run_type} profiled estimates for {nmi}')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
