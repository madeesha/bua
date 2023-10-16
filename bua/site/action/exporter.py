import csv
import os
import traceback
from typing import List, Optional, Dict, Callable

from pymysql import IntegrityError
from pymysql.cursors import SSDictCursor

from bua.facade.connection import DB
from bua.facade.s3 import S3
from bua.facade.sqs import Queue
from bua.site.action.accounts import Accounts
from bua.site.action.control import Control
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class Exporter(Accounts):

    def __init__(
            self, *,
            queue: Queue,
            conn: DB, log: Callable, debug: bool,
            s3: S3,
            batch_size=1
    ):
        Accounts.__init__(self, queue, conn, log, debug, batch_size)
        self.batch_size = batch_size
        self.s3 = s3

    def _reset_control_records(self, run_type: str, today: str, run_date: str, identifier_type: str):
        with self.conn.cursor() as cur:
            control = Control(run_type, today, today, today, run_date, identifier_type)
            control.reset_control_records(cur)
            self.conn.commit()

    def initiate_export_tables(
            self, table_names: List[str], partitions: List[str], batch_size: int,
            bucket_name: str, bucket_prefix: str, run_date: str, today: str, run_type: str, file_format: str
    ):
        identifier_type = f'Export {file_format}'
        self._reset_control_records(run_type, today, run_date, identifier_type)
        with self.conn.cursor() as cur:
            try:
                for table_name in table_names:
                    print(f'Exporting {table_name} to S3')
                    counter = 0
                    if partitions is not None:
                        for partition in partitions:
                            counter = self._initiate_export_table(
                                cur, table_name, partition, counter, batch_size,
                                bucket_name, bucket_prefix, run_date, today, run_type, file_format, identifier_type
                            )
                    else:
                        counter = self._initiate_export_table(
                            cur, table_name, None, counter, batch_size,
                            bucket_name, bucket_prefix, run_date, today, run_type, file_format, identifier_type
                        )
                    print(f'Initiate export of {counter} files to S3 for {table_name} to {bucket_prefix}')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def _initiate_export_table(
            self, cur: SSDictCursor, table_name: str, partition: Optional[str], counter: int, batch_size: int,
            bucket_name: str, bucket_prefix: str, run_date: str, today: str, run_type: str, file_format: str,
            identifier_type: str
    ) -> int:
        if partition is not None:
            cur.execute(f"SELECT COUNT(*) AS total FROM {table_name} PARTITION ({partition})")
        else:
            cur.execute(f"SELECT COUNT(*) AS total FROM {table_name}")
        total_rows = cur.fetchall()[0]['total']
        bodies = []
        body = None
        for offset in range(0, total_rows, batch_size):
            counter += 1
            if body is not None:
                self.queue.send_if_needed(bodies, force=False, batch_size=self.batch_size)
            body = {
                'table_name': table_name,
                'partition': partition,
                'counter': counter,
                'offset': offset,
                'batch_size': batch_size,
                'bucket_name': bucket_name,
                'bucket_prefix': bucket_prefix,
                'run_date': run_date,
                'run_type': run_type,
                'file_format': file_format,
                'identifier_type': identifier_type,
                'today': today
            }
            bodies.append(body)
        self.queue.send_if_needed(bodies, force=True, batch_size=self.batch_size)
        return counter

    def export_table(self, entry: Dict):
        try:
            with self.conn.cursor() as cur:
                table_name = entry['table_name']
                partition = entry['partition']
                counter = entry['counter']
                offset = entry['offset']
                batch_size = entry['batch_size']
                run_date = entry['run_date']
                bucket_name = entry['bucket_name']
                bucket_prefix = entry['bucket_prefix']
                file_format = entry['file_format']
                identifier_type = entry['identifier_type']
                run_type = entry['run_type']
                today = entry['today']
                control = Control(run_type, today, today, today, run_date, identifier_type)

                file_run_date = run_date[0:10].replace('-', '')
                file_name = f"BUA_{table_name}_{file_run_date}_{counter:05d}.{file_format}"
                file_path = f"/tmp/{file_name}"
                key = f'{bucket_prefix}{file_name}'
                if partition is not None:
                    sql = f"SELECT * FROM {table_name} PARTITION ({partition}) LIMIT {batch_size} OFFSET {offset}"
                else:
                    sql = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                if file_format == 'csv':
                    cur.execute(sql)
                    with open(file_path, 'w', newline='') as fp:
                        writer = csv.writer(fp, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        column_names = [col[0] for col in cur.description]
                        writer.writerow(column_names)
                        for record in cur.fetchall_unbuffered():
                            row = [record[name] for name in column_names]
                            writer.writerow(row)
                else:
                    return {
                        'status': STATUS_FAIL,
                        'cause': f'Unsupported file format {file_format}'
                    }
                if os.path.isfile(file_path):
                    with open(file_path, 'rb') as fp:
                        self.s3.upload_fileobj(fp=fp, bucket_name=bucket_name, key=key)
                    os.remove(file_path)
                    print(f'Uploaded {file_name} to {bucket_name}/{key}')
                    control.insert_control_record(self.conn, cur, file_name, STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
        except KeyError or IntegrityError as ex:
            traceback.print_exception(ex)
            return {
                'status': STATUS_FAIL,
                'cause': str(ex)
            }

    def initiate_prepare_export(
            self, run_type: str, today: str, run_date: str, identifier_type: str, end_inclusive: str
    ):
        self.queue_eligible_accounts(run_type, today, run_date, identifier_type, end_inclusive, all_accounts=True)

    def prepare_export(self, entry: Dict):
        try:
            with self.conn.cursor() as cur:
                account_id = entry['account_id']
                end_inclusive = entry['end_inclusive']
                stmt = f'CALL bua_prepare_export_data(%s, %s, %s, 1)'
                params = (account_id, account_id, end_inclusive)
                cur.execute(stmt, params)
                self.conn.commit()
                return {
                    'status': STATUS_DONE
                }
        except KeyError or IntegrityError as ex:
            traceback.print_exception(ex)
            return {
                'status': STATUS_FAIL,
                'cause': str(ex)
            }
