from typing import Dict

from bua.facade.s3 import S3
from bua.handler import DBLambdaHandler
from bua.site.action.exporter import Exporter


class BUASiteExportHandler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals data export"""

    def __init__(
            self, *,
            s3_client,
            sqs_client,
            ddb_bua_table,
            export_queue, failure_queue,
            conn,
            debug=False, max_receive_count=10
    ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table, conn=conn, debug=debug, failure_queue=failure_queue,
            max_receive_count=10
        )
        self.s3 = S3(s3_client=s3_client)
        self.export_queue = export_queue
        self._handler = {
            'ExportTables': self._handle_export_tables
        }

    def _handle_export_tables(self, _run_type: str, entry: Dict, debug: bool) -> Dict:
        action = Exporter(
            queue=self.export_queue,
            conn=self.conn, log=self.log, debug=debug,
            s3=self.s3
        )
        return action.export_table(entry)
