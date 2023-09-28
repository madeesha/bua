import os
import traceback
from typing import List, Optional

import pandas
from pymysql import Connection
from pymysql.cursors import Cursor

from bua.site.action import Action
from bua.site.handler import STATUS_DONE


class Exporter(Action):

    def __init__(self, queue, conn: Connection, debug=False, batch_size=100, s3_client=None, bucket_name=None):
        Action.__init__(self, queue, conn, debug)
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.batch_size = batch_size

    def initiate_export_tables(
            self, table_names: List[str], partitions: List[str], batch_size: int,
            bucket_prefix: str, run_date: str, run_type: str, index_col: str, file_format: str
    ):
        with self.conn.cursor() as cur:
            try:
                for table_name in table_names:
                    print(f'Exporting {table_name} to S3')
                    counter = 0
                    if partitions is not None:
                        for partition in partitions:
                            counter = self._initiate_export_table(
                                cur, table_name, partition, counter, batch_size,
                                bucket_prefix, run_date, run_type, index_col, file_format
                            )
                    else:
                        self._initiate_export_table(
                            cur, table_name, None, counter, batch_size,
                            bucket_prefix, run_date, run_type, index_col, file_format
                        )
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    def _initiate_export_table(
            self, cur: Cursor, table_name: str, partition: Optional[str], counter: int, batch_size: int,
            bucket_prefix: str, run_date: str, run_type: str, index_col: str, file_format: str
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
                'partition_name': partition,
                'counter': counter,
                'offset': offset,
                'batch_size': batch_size,
                'bucket_prefix': bucket_prefix,
                'run_date': run_date,
                'run_type': run_type,
                'index_col': index_col,
                'file_format': file_format
            }
            bodies.append(body)
        self.send_if_needed(bodies, force=True, batch_size=self.batch_size)
        return counter

    def export_table(
            self, table_name: str, partition: Optional[str], counter: int, offset: int, batch_size: int,
            bucket_prefix: str, run_date: str, index_col: str, file_format: str
    ):
        file_name = f"BUA_{table_name}_{run_date}_{counter}.{format}"
        file_path = f"/tmp/{file_name}"
        key = f'{bucket_prefix}/{file_name}'
        if partition is not None:
            sql = f"SELECT * FROM {table_name} PARTITION ({partition}) LIMIT {batch_size} OFFSET {offset}"
        else:
            sql = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        df = pandas.read_sql_query(sql, self.conn, index_col=index_col)
        if file_format == 'parquet':
            df.to_parquet(file_path, index=False)
        if file_format == 'csv':
            df.to_csv(file_path, index=False, header=True)
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as fp:
                self.s3_client.upload_fileobj(fp, self.bucket_name, key)
            os.remove(file_path)
            print(f'Uploaded {file_name} to {self.bucket_name}/{key}')
        return {
            'status': STATUS_DONE
        }

