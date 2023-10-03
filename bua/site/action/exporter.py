import os
import traceback
from typing import List, Optional, Dict

import pandas
from pymysql import Connection
from pymysql.cursors import Cursor
from sqlalchemy import Engine

from bua.site.action import Action
from bua.site.action.control import Control
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class Exporter(Action):

    def __init__(
            self, queue, conn: Connection,
            debug=False, batch_size=10,
            s3_client=None, bucket_name=None,
            engine: Optional[Engine] = None
    ):
        Action.__init__(self, queue, conn, debug)
        self.batch_size = batch_size
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.engine = engine

    def _reset_control_records(self, run_type: str, today: str, run_date: str, identifier_type: str):
        with self.conn.cursor() as cur:
            control = Control(run_type, today, today, today, run_date, identifier_type)
            control.reset_control_records(cur)
            self.conn.commit()

    def initiate_export_tables(
            self, table_names: List[str], partitions: List[str], batch_size: int,
            bucket_prefix: str, run_date: str, today: str, run_type: str, index_col: str, file_format: str
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
                                bucket_prefix, run_date, today, run_type, index_col, file_format, identifier_type
                            )
                    else:
                        counter = self._initiate_export_table(
                            cur, table_name, None, counter, batch_size,
                            bucket_prefix, run_date, today, run_type, index_col, file_format, identifier_type
                        )
                    print(f'Initiate export of {counter} files to S3 for {table_name} to {bucket_prefix}')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def _initiate_export_table(
            self, cur: Cursor, table_name: str, partition: Optional[str], counter: int, batch_size: int,
            bucket_prefix: str, run_date: str, today: str, run_type: str, index_col: str, file_format: str,
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
                self.send_if_needed(bodies, force=False, batch_size=self.batch_size)
            body = {
                'table_name': table_name,
                'partition': partition,
                'counter': counter,
                'offset': offset,
                'batch_size': batch_size,
                'bucket_prefix': bucket_prefix,
                'run_date': run_date,
                'run_type': run_type,
                'index_col': index_col,
                'file_format': file_format,
                'identifier_type': identifier_type,
                'today': today
            }
            bodies.append(body)
        self.send_if_needed(bodies, force=True, batch_size=self.batch_size)
        return counter

    def export_table(self, entry: Dict):
        try:
            with self.conn.cursor() as cur:
                table_name = entry['table_name']
                partition = entry['partition']
                counter = entry['counter']
                offset = entry['offset']
                batch_size = entry['batch_size']
                bucket_prefix = entry['bucket_prefix']
                run_date = entry['run_date']
                index_col = entry['index_col']
                file_format = entry['file_format']
                identifier_type = entry['identifier_type']
                run_type = entry['run_type']
                today = entry['today']
                control = Control(run_type, today, today, today, run_date, identifier_type)

                file_name = f"BUA_{table_name}_{run_date}_{counter}.{file_format}"
                file_path = f"/tmp/{file_name}"
                key = f'{bucket_prefix}/{file_name}'
                if partition is not None:
                    sql = f"SELECT * FROM {table_name} PARTITION ({partition}) LIMIT {batch_size} OFFSET {offset}"
                else:
                    sql = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                with self.engine.connect() as conn:
                    df = pandas.read_sql_query(sql, conn, index_col=index_col)
                    if file_format == 'parquet':
                        df.to_parquet(file_path, index=False)
                    if file_format == 'csv':
                        df.to_csv(file_path, index=False, header=True)
                    if os.path.isfile(file_path):
                        with open(file_path, 'rb') as fp:
                            self.s3_client.upload_fileobj(fp, self.bucket_name, key)
                        os.remove(file_path)
                        print(f'Uploaded {file_name} to {self.bucket_name}/{key}')
                        control.insert_control_record(self.conn, cur, file_name, STATUS_DONE)
                return {
                    'status': STATUS_DONE
                }
        except KeyError as ex:
            traceback.print_exception(ex)
            return {
                'status': STATUS_FAIL,
                'cause': str(ex)
            }
