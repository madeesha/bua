from typing import Dict
from bua.handler import DBLambdaHandler
from bua.site.action.basicread import BasicRead


class BUASiteBasicHandler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals basic read calculations"""

    def __init__(
            self, s3_client, meterdata_bucket_name,
            sqs_client, ddb_meterdata_table, ddb_bua_table,
            basic_queue, failure_queue,
            conn,
            debug=False
    ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table, conn=conn, debug=debug, failure_queue=failure_queue
        )
        self._s3_client = s3_client
        self._meterdata_bucket_name = meterdata_bucket_name
        self._meterdata_table = ddb_meterdata_table
        self._segment_queue = basic_queue
        self._handler = {
            'BasicRead': self._handle_basic_read,
            'ResetBasicRead': self._handle_reset_basic_read
        }

    def _handle_basic_read(self, run_type: str, entry: Dict, debug: bool) -> Dict:
        account_id = entry['account_id']
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        action = BasicRead(
            queue=self._segment_queue, conn=self.conn, log=self.log, debug=debug
        )
        return action.execute_basic_read_calculation(run_type, today, run_date, identifier_type, account_id)

    def _handle_reset_basic_read(self, run_type: str, entry: Dict, debug: bool) -> Dict:
        account_id = entry['account_id']
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        action = BasicRead(
            queue=self._segment_queue, conn=self.conn, log=self.log, debug=debug
        )
        return action.execute_reset_basic_read_calculation(run_type, today, run_date, identifier_type, account_id)
