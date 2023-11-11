import json

from bua.facade.connection import DBProxy
from bua.site.handler.initiate import BUASiteInitiateHandler
from tests.pipeline.stubs.ddb_table_stub import DDBTableStub
from tests.pipeline.stubs.s3_client_stub import S3ClientStub
from tests.pipeline.stubs.sqs_client_stub import SQSClientStub, SQSQueueStub
from tests.site.stubs import Database, MySQL


class TestClass:

    def test_initiate_utility(self):
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
        conn.unbuffered_result = [
            {
                'nmi': '123',
                'res_bus': 'RES',
                'jurisdiction': 'VIC',
                'tni': 'ABC',
                'nmi_suffix': 'E1',
                'stream_type': 'PRIMARY',
            }
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
        event = {
            'run_type': 'Utility',
            'run_date': '2023-10-01',
            'today': '2023-10-01',
            'source_date': '2023-10-01',
            'start_inclusive': '2022-10-01',
            'end_inclusive': '2023-09-30',
            'end_exclusive': '2023-10-01',
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
        assert len(data_queue.messages) == 1
        body = {
            'entries': [
                {
                    'run_type': 'Utility',
                    'run_date': '2023-10-01',
                    'today': '2023-10-01',
                    'source_date': '2023-10-01',
                    'start_inclusive': '2022-10-01',
                    'end_inclusive': '2023-09-30',
                    'end_exclusive': '2023-10-01',
                    'nmi': '123',
                    'res_bus': 'RES',
                    'jurisdiction': 'VIC',
                    'tni': 'ABC',
                    'stream_types': { 'E1': 'PRIMARY' },
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
        assert json.loads(data_queue.messages[0]['Entries'][0]['MessageBody']) == body
