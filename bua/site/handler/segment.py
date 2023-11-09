from typing import Dict

from bua.facade.connection import DBProxy
from bua.handler import DBLambdaHandler
from bua.site.action.check import Check
from bua.site.action.fix import Fix
from bua.site.action.sitesegment import SiteSegment


class BUASiteSegmentHandler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals profile segment calculations"""

    def __init__(
            self, s3_client, meterdata_bucket_name,
            sqs_client, ddb_meterdata_table, ddb_bua_table,
            segment_queue, failure_queue,
            conn: DBProxy, ctl_conn: DBProxy,
            debug=False, max_receive_count=10
    ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table,
            conn=conn, ctl_conn=ctl_conn,
            debug=debug, failure_queue=failure_queue,
            max_receive_count=max_receive_count
        )
        self._s3_client = s3_client
        self._meterdata_bucket_name = meterdata_bucket_name
        self._meterdata_table = ddb_meterdata_table
        self._segment_queue = segment_queue
        self._handler = {
            'SegmentJurisdiction': self._handle_segment_jurisdiction,
            'SegmentTNI': self._handle_segment_tni,
            'SegmentJurisdictionCheck': self._handle_segment_jurisdiction_check,
            'SegmentJurisdictionFix': self._handle_segment_jurisdiction_fix,
        }

    def _handle_segment_jurisdiction(self, _run_type: str, entry: Dict, debug: bool) -> Dict:
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        identifier_type: str = entry['identifier_type']
        avg_sum: str = entry['avg_sum']
        incl_est: bool = entry['incl_est']
        return self._calculate_jurisdiction_segment(
            identifier_type, run_date, source_date, entry, debug=debug, avg_sum=avg_sum, incl_est=incl_est
        )

    def _handle_segment_tni(self, _run_type: str, entry: Dict, debug: bool) -> Dict:
        run_date: str = entry['run_date']
        source_date: str = entry['source_date']
        identifier_type: str = entry['identifier_type']
        avg_sum: str = entry['avg_sum']
        incl_est: bool = entry['incl_est']
        return self._calculate_tni_segment(
            identifier_type, run_date, source_date, entry, debug=debug, avg_sum=avg_sum, incl_est=incl_est
        )

    def _handle_segment_jurisdiction_check(self, _run_type: str, entry: Dict, debug: bool) -> Dict:
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        interval_date = entry['interval_date']
        action = Check(
            queue=self._segment_queue, conn=self.conn, log=self.log, debug=debug
        )
        return action.segment_jurisdiction_check(run_date, identifier_type, interval_date)

    def _handle_segment_jurisdiction_fix(self, _run_type: str, entry: Dict, debug: bool) -> Dict:
        run_date = entry['run_date']
        identifier_type = entry['identifier_type']
        interval_date = entry['interval_date']
        action = Fix(
            queue=self._segment_queue, conn=self.conn, log=self.log, debug=debug
        )
        return action.segment_jurisdiction_fix(run_date, identifier_type, interval_date)

    def _calculate_jurisdiction_segment(self, identifier_type, run_date, source_date, entry,
                                        debug=False, avg_sum='Average', incl_est=True) -> Dict:
        site = SiteSegment(
            queue=self._segment_queue, conn=self.conn, log=self.log, debug=debug,
            meterdata_table=self._meterdata_table
        )
        jurisdiction_name = entry['jurisdiction_name']
        res_bus = entry['res_bus']
        stream_type = entry['stream_type']
        interval_date = entry['interval_date']
        return site.calculate_profile_segment(
            identifier_type=identifier_type, run_date=run_date,
            source_date=source_date,
            jurisdiction_name=jurisdiction_name,
            res_bus=res_bus, stream_type=stream_type, interval_date=interval_date,
            avg_sum=avg_sum, incl_est=incl_est
        )

    def _calculate_tni_segment(self, identifier_type, run_date, source_date, entry,
                               debug=False, avg_sum='Average', incl_est=True) -> Dict:
        site = SiteSegment(
            queue=self._segment_queue, conn=self.conn, log=self.log, debug=debug,
            meterdata_table=self._meterdata_table
        )
        jurisdiction_name = entry['jurisdiction_name']
        tni_name = entry['tni_name']
        res_bus = entry['res_bus']
        stream_type = entry['stream_type']
        interval_date = entry['interval_date']
        return site.calculate_profile_segment(
            identifier_type=identifier_type, run_date=run_date, source_date=source_date,
            jurisdiction_name=jurisdiction_name,
            tni_name=tni_name, res_bus=res_bus, stream_type=stream_type,
            interval_date=interval_date, avg_sum=avg_sum, incl_est=incl_est
        )
