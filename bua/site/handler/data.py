import json

from bua.site.action.sitedata import SiteData


class BUASiteDataHandler:
    """AWS Lambda handler for bottom up accruals site data extraction and validation"""
    def __init__(self, table, queue, conn, debug=False, check_nem=True, check_aggread=False):
        self.table = table
        self.queue = queue
        self.conn = conn
        self.debug = debug
        self.check_nem = check_nem
        self.check_aggread = check_aggread

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
                    run_type = entry['run_type']
                    run_date = entry['run_date']
                    source_date = entry['source_date']
                    if run_type == 'Utility':
                        self.extract_site_data(run_type, run_date, entry, debug)
                    elif run_type == 'Validate':
                        self.validate_site_data(run_type, run_date, source_date, entry, debug)

    def extract_site_data(self, run_type, run_date, entry, debug):
        site = SiteData(table=self.table, queue=self.queue, conn=self.conn, debug=debug)
        records = site.query_site_data(nmi=entry['nmi'],
                                       start_inclusive=entry['start_inclusive'],
                                       end_exclusive=entry['end_exclusive'])
        if debug:
            for record in records:
                print(record)
        site.insert_site_data(run_type=run_type, run_date=run_date,
                              nmi=entry['nmi'], res_bus=entry['res_bus'], jurisdiction=entry['jurisdiction'],
                              tni=entry['tni'], stream_types=entry['stream_types'],
                              start_inclusive=entry['start_inclusive'],
                              end_exclusive=entry['end_exclusive'], records=records)

    def validate_site_data(self, run_type, run_date, source_date, entry, debug):
        site = SiteData(
            table=self.table, queue=self.queue, conn=self.conn, debug=debug,
            check_nem=self.check_nem, check_aggread=self.check_aggread
        )
        site.validate_site_data(
            nmi=entry['nmi'],
            run_type=run_type,
            run_date=run_date,
            source_date=source_date,
            start_inclusive=entry['start_inclusive'],
            end_exclusive=entry['end_exclusive']
        )
