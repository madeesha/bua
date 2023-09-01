import json

from bua.site.action.check import Check
from bua.site.action.jurisdiction import SegmentJurisdiction
from bua.site.action.nem12 import NEM12
from bua.site.action.scalar import MicroScalar
from bua.site.action.sitedata import SiteData
from bua.site.action.tni import SegmentTNI
from bua.site.action.requeue import SiteRequeue


class BUASiteInitiateHandler:
    """AWS Lambda handler for bottom up accruals initiate site data extract"""
    def __init__(self, sqs_client, table, data_queue, segment_queue, conn,
                 debug=False, util_batch_size=10, jur_batch_size=5, tni_batch_size=10):
        self.sqs_client = sqs_client
        self.table = table
        self.data_queue = data_queue
        self.segment_queue = segment_queue
        self.conn = conn
        self.debug = debug
        self.util_batch_size = util_batch_size
        self.jur_batch_size = jur_batch_size
        self.tni_batch_size = tni_batch_size
        self._handlers = {
            'Utility': self._initiate_site_data_processing,
            'Validate': self._initiate_site_data_processing,
            'SegmentJurisdiction': self._initiate_segment_jurisdiction_calculation,
            'SegmentTNI': self._initiate_segment_tni_calculation,
            'Requeue': self._initiate_requeue,
            'NEM12': self._initiate_nem12_files,
            'MicroScalar': self._initiate_microscalar,
            'SegmentJurisdictionCheck': self._initiate_segment_jurisdiction_check,
        }
        self._initialise_connection()

    def reconnect(self, conn):
        self.conn = conn
        self._initialise_connection()

    def _initialise_connection(self):
        with self.conn.cursor() as cur:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 60")

    def handle_request(self, event):
        if 'Records' in event:
            for record in event['Records']:
                if record['eventSource'] == 'aws:sqs':
                    body = json.loads(record['body'])
                    self._process_message(body)
        else:
            self._process_message(event)

    def _process_message(self, body):
        debug = self.debug or body.get('debug') is True
        print(body)
        if 'run_type' in body:
            run_type: str = body['run_type']
            if run_type in self._handlers:
                self._handlers[run_type](body, debug, run_type)
                return
        print('Do not know what to do with this message')

    def _initiate_requeue(self, body, _debug, _run_type):
        site = SiteRequeue(sqs_client=self.sqs_client)
        site.initiate_requeue(
            source_queue_name=body['source_queue'],
            target_queue_name=body['target_queue']
        )

    def _initiate_segment_tni_calculation(self, body, debug, run_type):
        run_date: str = body['run_date']
        source_date: str = body['source_date']
        site = SegmentTNI(
            table=self.table, queue=self.segment_queue, conn=self.conn,
            debug=debug, batch_size=self.tni_batch_size
        )
        site.initiate_segment_tni_calculation(
            run_type=run_type,
            run_date=run_date,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            source_date=source_date
        )

    def _initiate_segment_jurisdiction_calculation(self, body, debug, run_type):
        run_date: str = body['run_date']
        source_date: str = body['source_date']
        site = SegmentJurisdiction(
            table=self.table, queue=self.segment_queue, conn=self.conn,
            debug=debug, batch_size=self.jur_batch_size
        )
        site.initiate_segment_jurisdiction_calculation(
            run_type=run_type,
            run_date=run_date,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            source_date=source_date
        )

    def _initiate_segment_jurisdiction_check(self, body, debug, run_type):
        run_date: str = body['run_date']
        today: str = body['today']
        identifier_type = body['identifier_type']
        site = Check(queue=self.segment_queue, conn=self.conn, debug=debug)
        site.initiate_segment_jurisdiction_check(
            run_type=run_type,
            run_date=run_date,
            today=today,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            identifier_type=identifier_type
        )

    def _initiate_site_data_processing(self, body, debug, run_type):
        run_date: str = body['run_date']
        source_date: str = body['source_date']
        site = SiteData(
            table=self.table, queue=self.data_queue, conn=self.conn,
            debug=debug, batch_size=self.util_batch_size
        )
        limit = body.get('limit', 1000000000)
        site.initiate_site_data_processing(
            run_type=run_type,
            run_date=run_date,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            source_date=source_date,
            limit=limit
        )

    def _initiate_nem12_files(self, body, debug, run_type):
        today: str = body['today']
        run_date: str = body['run_date']
        action = NEM12(queue=self.segment_queue, conn=self.conn, debug=debug)
        identifier_type = body['identifier_type']
        start_inclusive = body.get('start_inclusive')
        end_exclusive = body.get('end_exclusive')
        action.initiate_nem12_file_generation(
            run_type=run_type,
            today=today,
            run_date=run_date,
            start_inclusive=start_inclusive,
            end_exclusive=end_exclusive,
            identifier_type=identifier_type
        )

    def _initiate_microscalar(self, body, debug, run_type):
        today: str = body['today']
        run_date: str = body['run_date']
        action = MicroScalar(queue=self.segment_queue, conn=self.conn, debug=debug)
        identifier_type = body['identifier_type']
        action.initiate_microscalar_calculation(
            run_type=run_type,
            today=today,
            run_date=run_date,
            identifier_type=identifier_type
        )
