import base64
import decimal
import io
import traceback
from hashlib import md5
from typing import Optional, List, Dict, Set, Any
from datetime import datetime, timedelta, date

from pymysql import Connection
import csv
from bua.site.action import Action
from bua.site.action.control import Control
from bua.site.handler import STATUS_DONE, STATUS_FAIL


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
        generator = NEM12Generator(self.log, self.conn, self.s3_client, self.bucket_name, run_type, nmi,
                                   start_inclusive, end_exclusive, today, run_date, identifier_type)
        return generator.generate()


class NEM12Generator(Control):
    def __init__(
            self, log, conn: Connection, s3_client, bucket_name, run_type: str, nmi: str,
            start_inclusive: str, end_exclusive: str, today: str, run_date: str, identifier_type: str
    ):
        Control.__init__(self, run_type, nmi, start_inclusive, end_exclusive, today, run_date, identifier_type)
        self.log = log
        self.conn = conn
        self.s3_client = s3_client
        self.bucket_name = bucket_name

        self.file_date_time = datetime.strptime(self.run_date, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')
        self.update_date_time = datetime.strptime(self.run_date, '%Y-%m-%d %H:%M:%S')
        self.update_date_time = self.update_date_time - timedelta(days=36525)
        self.update_date_time = self.update_date_time.strftime('%Y%m%d%H%M%S')
        self.unique_id = f'{self.identifier}{self.file_date_time}'
        self.file_name = f'nem12#{self.unique_id}#bua#bua.csv'

        self.rows_counted = 0
        self.rows_written = 0
        self.status = 'PASS'
        self.reason = None
        self.extra = None
        self.key = None

        self.non_zero_counted = 0

        self.nmi_configurations: Dict[Any, Set] = dict()

        self.current_record = None
        self.current_read_values = self._default_read_values()

    @staticmethod
    def _default_read_values():
        return [decimal.Decimal(0) for _ in range(48)]

    def generate(self):
        with self.conn.cursor() as cur:
            try:
                decimal.getcontext().prec = 6
                records = self._fetch_missing_periods(cur)
                output = io.StringIO()
                writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(['100', 'NEM12', self.file_date_time, 'BUA', 'BUA'])
                self._calculate_nmi_configurations(records)
                for index, record in enumerate(records):
                    start_row = record['start_row']
                    end_row = record['end_row']
                    if not self._valid_register(record):
                        break
                    if not self._valid_suffix(record):
                        break
                    if not self._valid_uom(record):
                        break
                    scalar = self._convert_scalar(record)
                    if scalar is None:
                        break
                    if not self._valid_time_set(record):
                        break
                    is_new = self._is_new_reading(record)
                    if is_new:
                        self._construct_read_row(writer)
                        self.current_read_values = self._default_read_values()
                    self.current_record = record
                    values = [
                        decimal.Decimal(record[f'value_{index:02}']) * scalar
                        for index in range(start_row+1, end_row+1)
                    ]
                    self.current_read_values = [
                        *self.current_read_values[0:start_row],
                        *values,
                        *self.current_read_values[end_row:]
                    ]
                if self.current_record is not None:
                    self._construct_read_row(writer)
                if self.non_zero_counted == 0:
                    if self.reason is None:
                        self.reason = 'All rows are zero values'
                writer.writerow(['900'])
                if self.status == 'PASS':
                    self._write_nem12_file(self.file_name, output)
                self.log(f'{len(records)} {self.run_type} '
                         f'profiled estimates for '
                         f'{self.identifier}. {self.status} : {self.reason} : {self.extra}')
                self.conn.commit()
                self.insert_control_record(
                    self.conn, cur, self.status,
                    rows_counted=self.rows_counted, rows_written=self.rows_written,
                    reason=self.reason, extra=self.extra, key=self.key
                )
                return {
                    'status': STATUS_DONE
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                self.status = "FAIL"
                self.reason = "Exception raised"
                self.extra = str(ex)[0:255]
                self.insert_control_record(
                    self.conn, cur, self.status,
                    rows_counted=self.rows_counted, rows_written=self.rows_written,
                    reason=self.reason, extra=self.extra, key=self.key
                )
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex)
                }

    def _calculate_nmi_configurations(self, records: List[Dict]):
        for record in records:
            nmi_config = self.nmi_configurations.get(record['read_date'], set())
            self.nmi_configurations[record['read_date']] = nmi_config
            nmi_config.add(record['suffix_id'])

    def _is_new_reading(self, record: Dict):
        if self.current_record is None:
            return False
        if self.current_record['read_date'] != record['read_date']:
            return True
        if self.current_record['suffix_id'] != record['suffix_id']:
            return True
        if self.current_record['serial'] != record['serial']:
            return True
        if self.current_record['register_id'] != record['register_id']:
            return True
        if self.current_record['unit_of_measure'] != record['unit_of_measure']:
            return True
        return False

    def _fetch_missing_periods(self, cur):
        cur.execute(
            "CALL bua_list_missing_periods(%s, %s, %s, %s, %s, %s)",
            (self.identifier, self.start_inclusive, self.end_exclusive, self.today, self.run_date, self.identifier_type)
        )
        records: List[Dict] = list(cur.fetchall())
        self.rows_counted = len(records)
        if self.rows_counted == 0:
            self.reason = 'No missing reads'
        return records

    def _write_nem12_file(self, file_name, output):
        if self.rows_written > 0:
            self.key = f'nem/bua/{self.run_date}/{file_name}'
            body = output.getvalue().encode('utf-8')
            md5sum = base64.b64encode(md5(body).digest()).decode('utf-8')
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.key,
                ContentMD5=md5sum,
                ContentType='text/plain',
                Body=body,
                ContentLength=len(body)
            )

    def _valid_register(self, record: Dict):
        if len(record['register_id']) < 1:
            self.status = 'FAIL'
            self.reason = 'Invalid register id defined'
            suffix_id = record['suffix_id']
            serial = record['serial']
            read_date = record['read_date']
            self.extra = f'No register id defined for {self.identifier} {suffix_id} {serial} on {read_date}'
            return False
        return True

    def _valid_suffix(self, record: Dict):
        if not 1 <= len(record['suffix_id']) <= 2:
            self.status = 'FAIL'
            self.reason = 'Invalid suffix defined'
            serial = record['serial']
            register_id = record['register_id']
            read_date = record['read_date']
            self.extra = f'No suffix defined for {self.identifier} {serial} {register_id} on {read_date}'
            return False
        return True

    def _valid_uom(self, record):
        if len(record['unit_of_measure']) < 1:
            self.status = 'FAIL'
            self.reason = 'Invalid unit of measure defined'
            suffix_id = record['suffix_id']
            read_date = record['read_date']
            self.extra = f'No unit of measure defined for {self.identifier} {suffix_id} on {read_date}'
            return False
        return True

    def _convert_scalar(self, record):
        scalar = record['scalar']
        suffix_id = record['suffix_id']
        read_date = record['read_date']
        if scalar is None:
            self.status = 'FAIL'
            self.reason = 'No scalar defined'
            self.extra = f'No scalar defined for {self.identifier} {suffix_id} on {read_date}'
            return None
        scalar = decimal.Decimal(scalar)
        if scalar.is_zero():
            self.status = 'FAIL'
            self.reason = 'Scalar defined as zero'
            self.extra = f'Scalar defined as zero for {self.identifier} {suffix_id} on {read_date}'
            return None
        return scalar

    def _valid_time_set(self, record):
        time_set_id = record['time_set_id']
        start_row = record['start_row']
        end_row = record['end_row']
        if time_set_id is not None and start_row is None or end_row is None:
            self.status = 'FAIL'
            self.reason = 'No valid time periods'
            suffix_id = record['suffix_id']
            read_date = record['read_date']
            self.extra = f'No time periods for {self.identifier} {suffix_id} on {read_date}'
            return False
        return True

    def _construct_read_row(self, writer):
        total_value = sum(self.current_read_values)
        if total_value < 0:
            self.reason = 'Some rows have negative profile data'
        elif total_value > 0:
            self.non_zero_counted += 1
        if total_value >= 0:
            values = [f'{value:.06f}' for value in self.current_read_values]
            _interval_length = '30'
            _next_scheduled_read_date = ''
            _nmi_configuration = ''.join(self.nmi_configurations[self.current_record['read_date']])
            row = [
                '200', self.identifier, _nmi_configuration, self.current_record['register_id'],
                self.current_record['suffix_id'], self.current_record['suffix_id'], self.current_record['serial'],
                self.current_record['unit_of_measure'], _interval_length, _next_scheduled_read_date
            ]
            writer.writerow(row)
            _quality_method = 'AB'
            _reason_code = ''
            _reason_description = ''
            _msats_load_date_time = ''
            read_date = self.current_record['read_date'].strftime('%Y%m%d')
            writer.writerow([
                '300', read_date, *values, _quality_method, _reason_code, _reason_description,
                self.update_date_time, _msats_load_date_time
            ])
            self.rows_written += 1
