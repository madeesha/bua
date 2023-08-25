import json
from typing import Dict

from bua.site.action.nem12 import NEM12
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
            'SegmentJurisdictionAvgInclEst': self._handle_segment_jurisdiction_avg_incl_est,
            'SegmentJurisdictionSumInclEst': self._handle_segment_jurisdiction_sum_incl_est,
            'SegmentJurisdictionAvgExclEst': self._handle_segment_jurisdiction_avg_excl_est,
            'SegmentJurisdictionSumExclEst': self._handle_segment_jurisdiction_sum_excl_est,
            'SegmentTNIAvgInclEst': self._handle_segment_tni_avg_incl_est,
            'SegmentTNISumInclEst': self._handle_segment_tni_sum_incl_est,
            'SegmentTNIAvgExclEst': self._handle_segment_tni_avg_excl_est,
            'SegmentTNISumExclEst': self._handle_segment_tni_sum_excl_est,
            'NEM12': self._handle_nem12,
        }

    def reconnect(self, conn):
        self.conn = conn

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

    def _handle_segment_jurisdiction_avg_incl_est(self, run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        self.calculate_jurisdiction_segment(
            run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=True
        )

    def _handle_segment_jurisdiction_sum_incl_est(self, run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        self.calculate_jurisdiction_segment(
            run_type, run_date, source_date, entry, debug, avg_sum='Sum', incl_est=True
        )

    def _handle_segment_jurisdiction_avg_excl_est(self, run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        self.calculate_jurisdiction_segment(
            run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=False
        )

    def _handle_segment_jurisdiction_sum_excl_est(self, run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        self.calculate_jurisdiction_segment(
            run_type, run_date, source_date, entry, debug, avg_sum='Sum', incl_est=False
        )

    def _handle_segment_tni_avg_incl_est(self, run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        self.calculate_tni_segment(
            run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=True
        )

    def _handle_segment_tni_sum_incl_est(self, run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        self.calculate_tni_segment(
            run_type, run_date, source_date, entry, debug, avg_sum='Sum', incl_est=True
        )

    def _handle_segment_tni_avg_excl_est(self, run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        self.calculate_tni_segment(
            run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=False
        )

    def _handle_segment_tni_sum_excl_est(self, run_type: str, entry: Dict, debug: bool):
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        self.calculate_tni_segment(
            run_type, run_date, source_date, entry, debug, avg_sum='Sum', incl_est=False
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

    def calculate_jurisdiction_segment(self, run_type, run_date, source_date, entry,
                                       debug=False, avg_sum='Average', incl_est=True):
        site = SiteSegment(table=self.table, queue=self.queue, conn=self.conn, debug=debug)
        jurisdiction_name = entry['jurisdiction_name']
        res_bus = entry['res_bus']
        stream_type = entry['stream_type']
        interval_date = entry['interval_date']
        site.calculate_profile_segment(run_type=run_type, run_date=run_date, source_date=source_date,
                                       jurisdiction_name=jurisdiction_name,
                                       res_bus=res_bus, stream_type=stream_type, interval_date=interval_date,
                                       avg_sum=avg_sum, incl_est=incl_est)

    def calculate_tni_segment(self, run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=True):
        site = SiteSegment(table=self.table, queue=self.queue, conn=self.conn, debug=debug)
        jurisdiction_name = entry['jurisdiction_name']
        tni_name = entry['tni_name']
        res_bus = entry['res_bus']
        stream_type = entry['stream_type']
        interval_date = entry['interval_date']
        site.calculate_profile_segment(run_type=run_type, run_date=run_date, source_date=source_date,
                                       jurisdiction_name=jurisdiction_name,
                                       tni_name=tni_name, res_bus=res_bus, stream_type=stream_type,
                                       interval_date=interval_date, avg_sum=avg_sum, incl_est=incl_est)
