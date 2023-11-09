from bua.facade.connection import DBProxy
from bua.handler import DBLambdaHandler
from bua.site.action.sitedata import SiteData


class BUASiteDataHandler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals site data extraction and validation"""
    def __init__(self, s3_client, bucket_name,
                 sqs_client, ddb_meterdata_table, ddb_bua_table,
                 site_data_queue, failure_queue,
                 conn: DBProxy, ctl_conn: DBProxy,
                 debug=False, check_nem=True, check_aggread=False, max_receive_count=10
                 ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table,
            conn=conn, ctl_conn=ctl_conn,
            debug=debug,
            failure_queue=failure_queue, max_receive_count=max_receive_count
        )
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.ddb_meterdata_table = ddb_meterdata_table
        self.site_data_queue = site_data_queue
        self.check_nem = check_nem
        self.check_aggread = check_aggread
        self._handler = {
            'Utility': self._handle_utility,
            'Validate': self._handle_validate,
        }

    def _handle_utility(self, run_type, entry, debug):
        run_date = entry['run_date']
        site = SiteData(
            queue=self.site_data_queue, conn=self.conn, log=self.log, debug=debug,
            ddb_meterdata_table=self.ddb_meterdata_table
        )
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

    def _handle_validate(self, run_type, entry, debug):
        run_date = entry['run_date']
        source_date = entry['source_date']
        site = SiteData(
            queue=self.site_data_queue, conn=self.conn, log=self.log, debug=debug,
            ddb_meterdata_table=self.ddb_meterdata_table,
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
