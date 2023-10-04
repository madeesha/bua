from typing import Dict
from bua.handler import DBLambdaHandler
from bua.site.action.exporter import Exporter


class BUASiteExportHandler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals data export"""

    def __init__(
            self, s3_client, bua_bucket_name, sqs_client, ddb_bua_table, export_queue, failure_queue, conn,
            debug=False
    ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table, conn=conn, debug=debug, failure_queue=failure_queue
        )
        self.s3_client = s3_client
        self.bua_bucket_name = bua_bucket_name
        self.export_queue = export_queue
        self._handler = {
            'ExportTables': self._handle_export_tables
        }

    def _handle_export_tables(self, _run_type: str, entry: Dict, debug: bool) -> Dict:
        action = Exporter(
            queue=self.export_queue, conn=self.conn, log=self.log, debug=debug,
            s3_client=self.s3_client, bucket_name=self.bua_bucket_name
        )
        return action.export_table(entry)
