from typing import Dict
from zoneinfo import ZoneInfo

from bua.facade.connection import DBProxy
from bua.handler import DBLambdaHandler
from bua.site.action.nem12 import NEM12
from datetime import datetime


class BUASiteNEM12Handler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals NEM12 calculations"""

    def __init__(
            self, s3_client, meterdata_bucket_name,
            sqs_client, ddb_meterdata_table, ddb_bua_table,
            nem12_queue, failure_queue,
            conn: DBProxy, ctl_conn: DBProxy,
            debug=False, max_receive_count=10
    ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table,
            conn=conn, ctl_conn=ctl_conn,
            debug=debug,
            failure_queue=failure_queue, max_receive_count=max_receive_count
        )
        self._s3_client = s3_client
        self._meterdata_bucket_name = meterdata_bucket_name
        self._meterdata_table = ddb_meterdata_table
        self._segment_queue = nem12_queue
        self._handler = {
            'NEM12': self._handle_nem12,
            'ResetNEM12': self._handle_reset_nem12,
        }

    def _handle_nem12(self, run_type: str, entry: Dict, debug: bool) -> Dict:
        nmi = entry['nmi']
        start_inclusive = entry['start_inclusive']
        end_exclusive = entry['end_exclusive']
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        now = datetime.now(ZoneInfo('Australia/Sydney'))
        action = NEM12(
            queue=self._segment_queue, conn=self.conn, ctl_conn=self.ctl_conn, log=self.log, debug=debug,
            s3_client=self._s3_client, bucket_name=self._meterdata_bucket_name
        )
        return action.nem12_file_generation(
            run_type, nmi, start_inclusive, end_exclusive, today, run_date, identifier_type, now
        )

    def _handle_reset_nem12(self, run_type: str, entry: Dict, debug: bool) -> Dict:
        action = NEM12(
            queue=self._segment_queue, conn=self.conn, ctl_conn=self.ctl_conn, log=self.log, debug=debug,
            s3_client=self._s3_client, bucket_name=self._meterdata_bucket_name
        )
        today = entry['today']
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        start_inclusive = entry['start_inclusive']
        end_exclusive = entry['end_exclusive']
        account_id = entry['account_id']
        return action.reset_nem12(run_type, today, run_date, identifier_type, start_inclusive, end_exclusive, account_id)
