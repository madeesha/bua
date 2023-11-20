import json

from bua.facade.connection import DBProxy
from bua.site.handler.initiate import BUASiteInitiateHandler
from tests.pipeline.stubs.ddb_table_stub import DDBTableStub
from tests.pipeline.stubs.s3_client_stub import S3ClientStub
from tests.pipeline.stubs.sqs_client_stub import SQSClientStub, SQSQueueStub
from tests.site.stubs import Database, MySQL


class TestClass:

    def test_initiate_export(self):
        sqs_client = SQSClientStub(failure_queue_url='')
        s3_client = S3ClientStub()
        ddb_meterdata_table = DDBTableStub()
        ddb_bua_table = DDBTableStub()
        data_queue = SQSQueueStub()
        segment_queue = SQSQueueStub()
        export_queue = SQSQueueStub()
        failure_queue = SQSQueueStub()
        basic_queue = SQSQueueStub()
        mscalar_queue = SQSQueueStub()
        prepare_queue = SQSQueueStub()
        nem12_queue = SQSQueueStub()
        conn = Database(rowcount=10)
        conn.unbuffered_results = [
            [
                {
                    'total': 1
                }
            ],
            [
                {
                    'nmi': '123',
                    'res_bus': 'RES',
                    'jurisdiction': 'VIC',
                    'tni': 'ABC',
                    'nmi_suffix': 'E1',
                    'stream_type': 'PRIMARY',
                }
            ]
        ]
        ctl_conn = Database(rowcount=10)
        conn = DBProxy(mysql=MySQL(conn), username='test', password='test')
        ctl_conn = DBProxy(mysql=MySQL(ctl_conn), username='test', password='test')
        handler = BUASiteInitiateHandler(
            sqs_client=sqs_client,
            s3_client=s3_client,
            ddb_meterdata_table=ddb_meterdata_table, ddb_bua_table=ddb_bua_table,
            data_queue=data_queue, segment_queue=segment_queue, export_queue=export_queue, failure_queue=failure_queue,
            basic_queue=basic_queue, mscalar_queue=mscalar_queue, prepare_queue=prepare_queue, nem12_queue=nem12_queue,
            conn=conn, ctl_conn=ctl_conn
        )
        run_type = 'ExportTables'
        run_date = '2023-10-01'
        today = '2023-10-01'
        current_date = '2023-10-02'
        current_time = '10:11:12'
        source_date = '2023-10-01'
        start_inclusive = '2022-10-01'
        end_inclusive = '2023-09-30'
        end_exclusive = '2023-10-01'
        bucket_name = 'my-bucket-1'
        table_name = 'TableName'
        event = {
            'run_type': run_type,
            'run_date': run_date,
            'today': today,
            'current_date': current_date,
            'current_time': current_time,
            'source_date': source_date,
            'start_inclusive': start_inclusive,
            'end_inclusive': end_inclusive,
            'end_exclusive': end_exclusive,
            'bucket_name': bucket_name,
            'table_names': [
                table_name
            ],
            'db': {
                'prefix': 'tst',
                'update_id': '1',
                'suffix': 'sql',
                'domain': 'com',
                'schema': 'turkey',
            }
        }
        handler.handle_request(event)
        failure_queue.assert_no_messages()
        assert len(export_queue.messages) == 1
        body = {
            'entries': [
                {
                    'table_name': table_name,
                    'partition': None,
                    'counter': 1,
                    'offset': 0,
                    'batch_size': 1000000,
                    'bucket_name': bucket_name,
                    'bucket_prefix': 'export',
                    'run_date': run_date,
                    'run_type': run_type,
                    'file_format': 'csv',
                    'identifier_type': 'Export csv',
                    'today': today,
                    'current_date': current_date,
                    'current_time': current_time
                }
            ],
            'db': {
                'prefix': 'tst',
                'update_id': '1',
                'suffix': 'sql',
                'domain': 'com',
                'schema': 'turkey',
            }
        }
        assert json.loads(export_queue.messages[0]['Entries'][0]['MessageBody']) == body
