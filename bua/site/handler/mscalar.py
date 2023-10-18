from typing import Dict
from bua.handler import DBLambdaHandler
from bua.site.action.scalar import MicroScalar


class BUASiteMscalarHandler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals micro scalar calculations"""

    def __init__(
            self, s3_client, meterdata_bucket_name,
            sqs_client, ddb_meterdata_table, ddb_bua_table,
            mscalar_queue, failure_queue,
            conn,
            debug=False, max_receive_count=10
    ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table, conn=conn, debug=debug, failure_queue=failure_queue,
            max_receive_count=max_receive_count
        )
        self._s3_client = s3_client
        self._meterdata_bucket_name = meterdata_bucket_name
        self._meterdata_table = ddb_meterdata_table
        self._segment_queue = mscalar_queue
        self._handler = {
            'MicroScalar': self._handle_microscalar
        }

    def _handle_microscalar(self, run_type: str, entry: Dict, debug: bool) -> Dict:
        account_id = entry['account_id']
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        action = MicroScalar(
            queue=self._segment_queue, conn=self.conn, log=self.log, debug=debug
        )
        return action.execute_microscalar_calculation(run_type, today, run_date, identifier_type, account_id)
