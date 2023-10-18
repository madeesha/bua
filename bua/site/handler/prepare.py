from typing import Dict

from bua.facade.s3 import S3
from bua.handler import DBLambdaHandler
from bua.site.action.exporter import Exporter


class BUASitePrepareHandler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals prepare export calculations"""

    def __init__(
            self, *,
            sqs_client,
            s3_client,
            ddb_bua_table,
            prepare_queue, failure_queue,
            conn,
            debug=False, max_receive_count=10
    ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table, conn=conn, debug=debug, failure_queue=failure_queue,
            max_receive_count=max_receive_count
        )
        self.prepare_queue = prepare_queue
        self.s3 = S3(s3_client=s3_client)
        self._handler = {
            'PrepareExport': self._handle_prepare_export,
        }

    def _handle_prepare_export(self, _run_type: str, entry: Dict, debug: bool) -> Dict:
        action = Exporter(
            queue=self.prepare_queue, conn=self.conn, log=self.log, debug=debug,
            s3=self.s3
        )
        return action.prepare_export(entry)
