import json
from typing import Dict

from bua.site.action.basicread import BasicRead
from bua.site.action.check import Check
from bua.site.action.nem12 import NEM12
from bua.site.action.scalar import MicroScalar
from bua.site.action.sitesegment import SiteSegment


class BUASiteSegmentHandler:
    """AWS Lambda handler for bottom up accruals profile segment calculations"""
    def __init__(self, s3_client, bucket_name, table, queue, conn, debug=False):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.table = table
        self.queue = queue
        self.conn = conn
        self.debug = debug
        self._handler = {
            'SegmentJurisdiction': self._handle_segment_jurisdiction,
            'SegmentTNI': self._handle_segment_tni,
            'SegmentJurisdictionCheck': self._handle_segment_jurisdiction_check,
            'NEM12': self._handle_nem12,
            'MicroScalar': self._handle_microscalar,
            'BasicRead': self._handle_basic_read,
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
        if 'entries' in body:
            for entry in body['entries']:
                if 'run_type' in entry:
                    run_type: str = entry['run_type']
                    if run_type in self._handler:
                        self._handler[run_type](run_type, entry, debug)

    def _handle_segment_jurisdiction(self, _run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        identifier_type: str = entry['identifier_type']
        avg_sum: str = entry['avg_sum']
        incl_est: bool = entry['incl_est']
        self.calculate_jurisdiction_segment(
            identifier_type, run_date, source_date, entry, debug=debug, avg_sum=avg_sum, incl_est=incl_est
        )

    def _handle_segment_tni(self, _run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        identifier_type: str = entry['identifier_type']
        avg_sum: str = entry['avg_sum']
        incl_est: bool = entry['incl_est']
        self.calculate_tni_segment(
            identifier_type, run_date, source_date, entry, debug=debug, avg_sum=avg_sum, incl_est=incl_est
        )

    def _handle_nem12(self, run_type: str, entry: Dict, debug: bool):
        nmi = entry['nmi']
        start_inclusive = entry['start_inclusive']
        end_exclusive = entry['end_exclusive']
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        action = NEM12(
            queue=self.queue, conn=self.conn, debug=debug, s3_client=self.s3_client, bucket_name=self.bucket_name
        )
        action.nem12_file_generation(run_type, nmi, start_inclusive, end_exclusive, today, run_date, identifier_type)

    def _handle_microscalar(self, run_type: str, entry: Dict, debug: bool):
        account_id = entry['account_id']
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        action = MicroScalar(
            queue=self.queue, conn=self.conn, debug=debug
        )
        action.execute_microscalar_calculation(run_type, today, run_date, identifier_type, account_id)

    def _handle_basic_read(self, run_type: str, entry: Dict, debug: bool):
        account_id = entry['account_id']
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        action = BasicRead(
            queue=self.queue, conn=self.conn, debug=debug
        )
        action.execute_basic_read_calculation(run_type, today, run_date, identifier_type, account_id)

    def _handle_segment_jurisdiction_check(self, _run_type: str, entry: Dict, debug: bool):
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        interval_date = entry['interval_date']
        action = Check(queue=self.queue, conn=self.conn, debug=debug)
        action.segment_jurisdiction_check(run_date, identifier_type, interval_date)

    def calculate_jurisdiction_segment(self, identifier_type, run_date, source_date, entry,
                                       debug=False, avg_sum='Average', incl_est=True):
        site = SiteSegment(table=self.table, queue=self.queue, conn=self.conn, debug=debug)
        jurisdiction_name = entry['jurisdiction_name']
        res_bus = entry['res_bus']
        stream_type = entry['stream_type']
        interval_date = entry['interval_date']
        site.calculate_profile_segment(identifier_type=identifier_type, run_date=run_date, source_date=source_date,
                                       jurisdiction_name=jurisdiction_name,
                                       res_bus=res_bus, stream_type=stream_type, interval_date=interval_date,
                                       avg_sum=avg_sum, incl_est=incl_est)

    def calculate_tni_segment(self, identifier_type, run_date, source_date, entry,
                              debug=False, avg_sum='Average', incl_est=True):
        site = SiteSegment(table=self.table, queue=self.queue, conn=self.conn, debug=debug)
        jurisdiction_name = entry['jurisdiction_name']
        tni_name = entry['tni_name']
        res_bus = entry['res_bus']
        stream_type = entry['stream_type']
        interval_date = entry['interval_date']
        site.calculate_profile_segment(identifier_type=identifier_type, run_date=run_date, source_date=source_date,
                                       jurisdiction_name=jurisdiction_name,
                                       tni_name=tni_name, res_bus=res_bus, stream_type=stream_type,
                                       interval_date=interval_date, avg_sum=avg_sum, incl_est=incl_est)
