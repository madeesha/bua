import base64
import decimal
import io
import traceback
from hashlib import md5
from typing import Optional, List, Dict, Set, Any, Callable
from datetime import datetime, timedelta, date
import csv

from pymysql import InternalError, InterfaceError, DatabaseError

from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action.accounts import Accounts
from bua.site.action.control import Control
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class NEM12(Accounts):

    def __init__(
            self, queue: Queue, conn: DB, ctl_conn: DB, log: Callable, debug: bool, batch_size=100,
            s3_client=None, bucket_name=None
    ):
        Accounts.__init__(self, queue, conn, ctl_conn, log, debug, batch_size)
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
        control = Control(self.ctl_conn, run_type, start_inclusive, end_exclusive, today, run_date, identifier_type)
        control.reset_control_records()
        self._prepare_nem12_files_to_process(end_exclusive, identifier_type, run_date, run_type, start_inclusive, today)
        self._queue_nem12_files_to_process(control, identifier_type, run_date, run_type, today)

    def initiate_reset_nem12(self, run_type: str, today: str, run_date: str, identifier_type: str,
                             start_inclusive: str, end_exclusive: str, end_inclusive: str,
                             proc_name=None):
        self.reset_control_records(run_type, today, run_date, identifier_type)
        self.queue_eligible_accounts(run_type, today, run_date, identifier_type,
                                     start_inclusive, end_exclusive, end_inclusive,
                                     all_accounts=False, proc_name=proc_name)

    def _queue_nem12_files_to_process(self, control: Control, identifier_type, run_date, run_type, today):
        with self.conn.cursor() as cur:
            try:
                stmt = """
                SELECT identifier, start_inclusive, end_exclusive 
                FROM BUAControl 
                WHERE run_type = %s AND run_date = %s AND status = 'PREP'
                """
                params = (run_type, run_date)
                cur.execute(stmt, params)
                total = 0
                bodies = []
                body = None
                for record in cur.fetchall_unbuffered():
                    nmi = record['identifier']
                    start_date: date = record['start_inclusive']
                    end_date: date = record['end_exclusive']
                    if start_date <= end_date:
                        if body is not None:
                            self.queue.send_if_needed(bodies, force=False, batch_size=self.batch_size)
                        body = {
                            'run_type': run_type,
                            'today': today,
                            'run_date': run_date,
                            'nmi': nmi,
                            'start_inclusive': start_date.strftime('%Y-%m-%d'),
                            'end_exclusive': end_date.strftime('%Y-%m-%d'),
                            'identifier_type': identifier_type
                        }
                        bodies.append(body)
                        total += 1
                    else:
                        control.update_control_record(
                            identifier=nmi, status='SKIP', reason='Start date after end date', commit=False,
                            start_inclusive=start_date
                        )
                self.queue.send_if_needed(bodies, force=True, batch_size=self.batch_size)
                self.log(f'{total} sites to generate {run_type} profiled estimates data')
                self.conn.commit()
                control.commit()
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def _prepare_nem12_files_to_process(self, end_exclusive, identifier_type, run_date, run_type, start_inclusive,
                                        today):
        with self.conn.cursor() as cur:
            try:
                stmt = "CALL bua_prep_profile_nmis(%s, %s, %s, %s, %s, %s)"
                params = (start_inclusive, end_exclusive, today, run_date, run_type, identifier_type)
                total = cur.execute(stmt, params)
                self.log(f'{total} sites prepared to generate {run_type} profiled estimates data')
                self.conn.commit()
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def nem12_file_generation(
            self, run_type: str, nmi: str, start_inclusive: str, end_exclusive: str,
            today: str, run_date: str, identifier_type: str, now: datetime
    ):
        generator = NEM12Generator(self.log, self.conn, self.ctl_conn, self.s3_client, self.bucket_name, run_type, nmi,
                                   start_inclusive, end_exclusive, today, run_date, identifier_type, now)
        return generator.generate_file()

    def reset_nem12(
            self, run_type: str, today: str, run_date: str, identifier_type: str,
            start_inclusive: str, end_exclusive: str, account_id: int
    ):
        control = Control(self.ctl_conn, run_type, start_inclusive, end_exclusive, today, run_date, identifier_type)
        with self.conn.cursor() as cur:
            sql = """
            DELETE FROM AggregatedRead 
            WHERE account_id = %s 
            AND cr_date >= %s 
            AND COALESCE(cr_process, '') != 'BUA_BASIC' 
            AND invoice_run_id = -1
            """
            params = (account_id, run_date)
            try:
                self.log(f'Executing {run_type} for account {account_id} on {run_date}')
                cur.execute(sql, params)
                self.conn.commit()
                control.update_control_record(str(account_id), STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except DatabaseError as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.update_control_record(str(account_id), STATUS_FAIL, reason='DatabaseError', extra=str(ex))
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex),
                    'context': {
                        'sql': sql,
                        'params': list(params),
                    }
                }
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                control.update_control_record(str(account_id), STATUS_FAIL, reason='UnhandledError', extra=str(ex))
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex),
                    'context': {
                        'sql': sql,
                        'params': list(params),
                    }
                }


class NEM12Status:
    def __init__(self):
        self.status: str = 'PASS'
        self.reason: Optional[str] = None
        self.extra: Optional[str] = None

    def update(self, status: Optional[str] = None, reason: Optional[str] = None, extra: Optional[str] = None):
        if status is not None:
            self.status = status
        if reason is not None:
            self.reason = reason[0:255]
        if extra is not None:
            self.extra = extra[0:255]


class NEM12Generator:
    def __init__(
            self, log, conn: DB, ctl_conn: DB, s3_client, bucket_name, run_type: str, nmi: str,
            start_inclusive: str, end_exclusive: str, today: str, run_date: str, identifier_type: str,
            now: datetime
    ):
        self.control = Control(ctl_conn, run_type, start_inclusive, end_exclusive, today, run_date, identifier_type)
        self.identifier = nmi
        self.log = log
        self.conn = conn
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.run_date = run_date
        self.run_type = run_type
        self.start_inclusive = start_inclusive
        self.end_exclusive = end_exclusive
        self.today = today
        self.identifier_type = identifier_type

        self.file_date_time = now.strftime('%Y%m%d%H%M')
        years_ago = now - timedelta(days=36525)
        self.update_date_time = years_ago.strftime('%Y%m%d%H%M%S')
        self.unique_id = f'{self.identifier}{self.file_date_time}'
        self.file_name = f'nem12#{self.unique_id}#bua#bua.csv'

        self.rows_counted = 0

        self.state = NEM12Status()

        self.s3_key = None

        self.generator = NEM12ContentGenerator(
            file_date_time=self.file_date_time,
            update_date_time=self.update_date_time,
            identifier=self.identifier
        )

    def generate_file(self):
        with self.conn.cursor() as cur:
            try:
                records = self._fetch_missing_periods(cur)

                output = self.generator.generate_nem12_file_content(records, self.state)

                if self.state.status == 'PASS':
                    self._write_nem12_file(output)
                self.log(f'{len(records)} {self.run_type} '
                         f'profiled estimates for '
                         f'{self.identifier}. {self.state.status} : {self.state.reason} : {self.state.extra}')
                self.conn.commit()
                self.control.update_control_record(
                    self.identifier, self.state.status,
                    rows_counted=self.rows_counted, rows_written=self.generator.rows_written,
                    reason=self.state.reason, extra=self.state.extra, key=self.s3_key,
                    start_inclusive=self.start_inclusive
                )
                return {
                    'status': STATUS_DONE
                }
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                self.state.update('FAIL', 'Exception raised', str(ex))
                self.control.update_control_record(
                    self.identifier, self.state.status,
                    rows_counted=self.rows_counted, rows_written=self.generator.rows_written,
                    reason=self.state.reason, extra=self.state.extra, key=self.s3_key,
                    start_inclusive=self.start_inclusive
                )
                return {
                    'status': STATUS_FAIL,
                    'cause': str(ex)
                }

    def _fetch_missing_periods(self, cur) -> List[Dict]:
        cur.execute(
            "CALL bua_list_missing_periods(%s, %s, %s, %s, %s, %s)",
            (self.identifier, self.start_inclusive, self.end_exclusive, self.today, self.run_date, self.identifier_type)
        )
        records: List[Dict] = list(cur.fetchall())
        self.rows_counted = len(records)
        if self.rows_counted == 0:
            self.state.update(reason='No missing reads')
        return records

    def _write_nem12_file(self, output):
        if self.generator.rows_written > 0:
            self.s3_key = f'nem/bua/{self.run_date}/{self.file_name}'
            body = output.getvalue().encode('utf-8')
            md5sum = base64.b64encode(md5(body).digest()).decode('utf-8')
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.s3_key,
                ContentMD5=md5sum,
                ContentType='text/plain',
                Body=body,
                ContentLength=len(body)
            )


class NEM12ContentGenerator:

    def __init__(self, *, file_date_time, update_date_time, identifier):
        self.file_date_time = file_date_time
        self.update_date_time = update_date_time
        self.identifier = identifier
        self.nmi_configurations: Dict[Any, Set] = dict()
        self.non_zero_counted = 0
        self.rows_written = 0
        self.current_record = None
        self.current_read_values = None
        self.state = NEM12Status()

    def generate_nem12_file_content(self, records: List, state: Optional[NEM12Status] = None):
        if state is not None:
            self.state = state
        decimal.getcontext().prec = 6
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
            is_first = self.current_record is None
            if is_first:
                self.current_read_values = self._default_read_values(record)
            is_new = self._is_new_reading(record)
            if is_new:
                self._construct_read_row(writer)
                self.current_read_values = self._default_read_values(record)
            self.current_record = record
            if start_row is not None and end_row is not None:
                values = [
                    decimal.Decimal(record[f'value_{index:02}']) * scalar
                    for index in range(start_row + 1, end_row + 1)  # start_row zero based , end_row exclusive
                ]
                self.current_read_values = [
                    *self.current_read_values[0:start_row],
                    *values,
                    *self.current_read_values[end_row:]
                ]
        if self.current_record is not None:
            self._construct_read_row(writer)
        if self.non_zero_counted == 0:
            if self.state.reason is None:
                self.state.update(reason='All rows are zero values')
        writer.writerow(['900'])
        return output

    def _calculate_nmi_configurations(self, records: List[Dict]):
        for record in records:
            nmi_config = self.nmi_configurations.get(record['read_date'], set())
            self.nmi_configurations[record['read_date']] = nmi_config
            nmi_config.add(record['suffix_id'])

    def _valid_register(self, record: Dict):
        if len(record['register_id']) < 1:
            suffix_id = record['suffix_id']
            serial = record['serial']
            read_date = record['read_date']
            self.state.update(
                'FAIL', 'Invalid register id defined',
                f'No register id defined for {self.identifier} {suffix_id} {serial} on {read_date}'
            )
            return False
        return True

    def _valid_suffix(self, record: Dict):
        if not 1 <= len(record['suffix_id']) <= 2:
            serial = record['serial']
            register_id = record['register_id']
            read_date = record['read_date']
            self.state.update(
                'FAIL', 'Invalid suffix defined',
                f'No suffix defined for {self.identifier} {serial} {register_id} on {read_date}'
            )
            return False
        return True

    def _valid_uom(self, record):
        if len(record['unit_of_measure']) < 1:
            suffix_id = record['suffix_id']
            read_date = record['read_date']
            self.state.update(
                'FAIL', 'Invalid unit of measure defined',
                f'No unit of measure defined for {self.identifier} {suffix_id} on {read_date}'
            )
            return False
        return True

    @staticmethod
    def _convert_scalar(record):
        scalar = record['scalar']
        if scalar is None:
            return decimal.Decimal(1)
        scalar = decimal.Decimal(scalar)
        if scalar <= decimal.Decimal(0):
            return decimal.Decimal(1)
        return scalar

    @staticmethod
    def _default_read_values(record: Dict):
        return [
            decimal.Decimal(record[f'value_{index:02}'])
            for index in range(1, 49)
        ]

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

    def _construct_read_row(self, writer):
        total_value = sum(self.current_read_values)
        if total_value < 0:
            self.state.update(reason='Some rows have negative profile data')
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
