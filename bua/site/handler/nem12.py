from typing import Dict
from bua.handler import DBLambdaHandler
from bua.site.action.nem12 import NEM12


class BUASiteNEM12Handler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals NEM12 calculations"""

    def __init__(
            self, s3_client, meterdata_bucket_name,
            sqs_client, ddb_meterdata_table, ddb_bua_table,
            nem12_queue, failure_queue,
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
        self._segment_queue = nem12_queue
        self._handler = {
            'NEM12': self._handle_nem12
        }

    def _handle_nem12(self, run_type: str, entry: Dict, debug: bool) -> Dict:
        nmi = entry['nmi']
        start_inclusive = entry['start_inclusive']
        end_exclusive = entry['end_exclusive']
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        action = NEM12(
            queue=self._segment_queue, conn=self.conn, log=self.log, debug=debug,
            s3_client=self._s3_client, bucket_name=self._meterdata_bucket_name
        )
        return action.nem12_file_generation(
            run_type, nmi, start_inclusive, end_exclusive, today, run_date, identifier_type
        )
