import base64
import decimal
import io
import traceback
from hashlib import md5
from typing import Optional, List, Dict
from datetime import datetime, timedelta, date

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
                    "DELETE FROM BUAControl WHERE run_type = %s AND run_date = %s",
                    (run_type, run_date)
                )
                cur.execute(
                    "CALL bua_list_profile_nmis(%s, %s, %s, %s)",
                    (start_inclusive, end_exclusive, today, run_date)
                )
                total = 0
                bodies = []
                body = None
                for record in cur.fetchall_unbuffered():
                    nmi = record['nmi']
                    start_date: date = record['start_inclusive']
                    end_date: date = record['end_exclusive']
                    if start_date <= end_date:
                        if body is not None:
                            self.send_if_needed(bodies, force=False, batch_size=self.batch_size)
                        body = {
                            'run_type': run_type,
                            'today': today,
                            'run_date': run_date,
                            'nmi': nmi,
                            'start_inclusive': start_date.strftime('%Y%m%d'),
                            'end_exclusive': end_date.strftime('%Y%m%d'),
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

    def nem12_file_generation(
            self, run_type: str, nmi: str, start_inclusive: str, end_exclusive: str,
            today: str, run_date: str, identifier_type: str
    ):
        rows_counted = None
        rows_written = None
        key = None
        with self.conn.cursor() as cur:
            try:
                decimal.getcontext().prec = 6
                cur.execute(
                    "CALL bua_list_missing_periods(%s, %s, %s, %s, %s, %s)",
                    (nmi, start_inclusive, end_exclusive, today, run_date, identifier_type)
                )
                records: List[Dict] = list(cur.fetchall())
                rows_counted = len(records)
                file_date_time = datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')
                update_date_time = datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S')
                update_date_time = update_date_time - timedelta(days=36525)
                update_date_time = update_date_time.strftime('%Y%m%d%H%M%S')
                unique_id = f'{nmi}{file_date_time}'
                file_name = f'nem12#{unique_id}#bua#bua.csv'
                output = io.StringIO()
                writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(['100', 'NEM12', file_date_time, 'BUA', 'BUA'])
                nmi_configuration = ''.join({row['suffix_id'] for row in records})
                rows_written = 0
                reason = None
                extra = None
                status = 'PASS'
                if rows_counted == 0:
                    reason = 'No missing reads'
                for index, record in enumerate(records):
                    register_id = record['register_id']
                    suffix_id = record['suffix_id']
                    serial = record['serial']
                    unit_of_measure = record['unit_of_measure']
                    read_date = record['read_date'].strftime('%Y%m%d')
                    if register_id is None:
                        status = 'FAIL'
                        reason = 'No register id defined'
                        extra = f'No register id defined for {nmi} {suffix_id} {serial} on {read_date}'
                        break
                    if suffix_id is None:
                        status = 'FAIL'
                        reason = 'No suffix defined'
                        extra = f'No suffix defined for {nmi} {serial} {register_id} on {read_date}'
                        break
                    if unit_of_measure is None:
                        status = 'FAIL'
                        reason = 'No unit of measure defined'
                        extra = f'No unit of measure defined for {nmi} {suffix_id} on {read_date}'
                        break
                    if record['scalar'] is None:
                        status = 'FAIL'
                        reason = 'No scalar defined'
                        extra = f'No scalar defined for {nmi} {suffix_id} on {read_date}'
                        break
                    scalar = decimal.Decimal(record['scalar'])
                    if scalar.is_zero():
                        status = 'FAIL'
                        reason = 'Scalar defined as zero'
                        extra = f'Scalar defined as zero for {nmi} {suffix_id} on {read_date}'
                        break
                    if index > 0:
                        if records[index-1]['read_date'] == record['read_date']:
                            if records[index-1]['suffix_id'] == record['suffix_id']:
                                status = 'FAIL'
                                reason = 'Multiple scalar values defined'
                                extra = f'Multiple scalar values defined for {nmi} {suffix_id} on {read_date}'
                                break
                    values = [decimal.Decimal(record[f'value_{index:02}']) * scalar for index in range(1, 49)]
                    total_value = sum(values)
                    if total_value == 0:
                        if reason is None:
                            reason = 'Some rows have zero profile data'
                    elif total_value < 0:
                        reason = 'Some rows have negative profile data'
                    if total_value >= 0:
                        values = [f'{value:.06f}' for value in values]
                        row = [
                            '200', nmi, nmi_configuration, register_id, suffix_id, suffix_id, serial,
                            unit_of_measure, '30', ''
                        ]
                        writer.writerow(row)
                        writer.writerow(['300', read_date, *values, 'AB', '', '', update_date_time, ''])
                        rows_written += 1
                writer.writerow(['900'])
                if status == 'PASS':
                    if rows_written > 0:
                        key = f'bua/{run_date}/{file_name}'
                        body = output.getvalue().encode('utf-8')
                        md5sum = base64.b64encode(md5(body).digest()).decode('utf-8')
                        self.s3_client.put_object(
                            Bucket=self.bucket_name,
                            Key=key,
                            ContentMD5=md5sum,
                            ContentType='text/plain',
                            Body=body,
                            ContentLength=len(body)
                        )
                sql = """
                INSERT INTO BUAControl
                ( run_type, identifier, start_inclusive, end_exclusive, today, run_date
                , identifier_type, rows_counted, rows_written, status, reason, extra, s3_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql,
                (run_type, nmi, start_inclusive, end_exclusive, today, run_date,
                     identifier_type, rows_counted, rows_written, status, reason, extra, key)
                )
                self.log(f'{len(records)} {run_type} profiled estimates for {nmi}. {status} : {reason} : {extra}')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                try:
                    status = "FAIL"
                    reason = "Exception raised"
                    extra = str(ex)[0:255]
                    sql = """
                    INSERT INTO BUAControl
                    ( run_type, identifier, start_inclusive, end_exclusive, today, run_date
                    , identifier_type, rows_counted, rows_written, status, reason, extra, s3_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cur.execute(sql,
                    (run_type, nmi, start_inclusive, end_exclusive, today, run_date,
                         identifier_type, rows_counted, rows_written, status, reason, extra, key)
                    )
                    self.conn.commit()
                except Exception as e2:
                    traceback.print_exception(e2)
                    self.conn.rollback()
