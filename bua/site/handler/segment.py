import json

from bua.site.action.sitesegment import SiteSegment


class BUASiteSegmentHandler:
    """AWS Lambda handler for bottom up accruals profile segment calculations"""
    def __init__(self, table, queue, conn, debug=False):
        self.table = table
        self.queue = queue
        self.conn = conn
        self.debug = debug

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
                if 'run_type' in entry and 'run_date' in entry and 'source_date' in entry:
                    run_type: str = entry['run_type']
                    run_date: str = entry['run_date']
                    source_date: str = entry['source_date']
                    if run_type == 'SegmentJurisdictionAvgInclEst':
                        self.calculate_jurisdiction_segment(
                            run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=True
                        )
                    elif run_type == 'SegmentJurisdictionSumInclEst':
                        self.calculate_jurisdiction_segment(
                            run_type, run_date, source_date, entry, debug, avg_sum='Sum', incl_est=True
                        )
                    elif run_type == 'SegmentJurisdictionAvgExclEst':
                        self.calculate_jurisdiction_segment(
                            run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=False
                        )
                    elif run_type == 'SegmentJurisdictionSumExclEst':
                        self.calculate_jurisdiction_segment(
                            run_type, run_date, source_date, entry, debug, avg_sum='Sum', incl_est=False
                        )
                    elif run_type == 'SegmentTNIAvgInclEst':
                        self.calculate_tni_segment(
                            run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=True
                        )
                    elif run_type == 'SegmentTNISumInclEst':
                        self.calculate_tni_segment(
                            run_type, run_date, source_date, entry, debug, avg_sum='Sum', incl_est=True
                        )
                    elif run_type == 'SegmentTNIAvgExclEst':
                        self.calculate_tni_segment(
                            run_type, run_date, source_date, entry, debug, avg_sum='Average', incl_est=False
                        )
                    elif run_type == 'SegmentTNISumExclEst':
                        self.calculate_tni_segment(
                            run_type, run_date, source_date, entry, debug, avg_sum='Sum', incl_est=False
                        )

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
