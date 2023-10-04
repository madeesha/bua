from bua.facade.sqs import Queue
from bua.handler import DBLambdaHandler
from bua.site.action.basicread import BasicRead
from bua.site.action.check import Check
from bua.site.action.exporter import Exporter
from bua.site.action.fix import Fix
from bua.site.action.jurisdiction import SegmentJurisdiction
from bua.site.action.nem12 import NEM12
from bua.site.action.scalar import MicroScalar
from bua.site.action.sitedata import SiteData
from bua.site.action.tni import SegmentTNI
from bua.site.action.requeue import SiteRequeue


class BUASiteInitiateHandler(DBLambdaHandler):
    """AWS Lambda handler for bottom up accruals initiate site data extract"""
    def __init__(
            self, sqs_client, ddb_meterdata_table, ddb_bua_table,
            data_queue, segment_queue, export_queue, failure_queue,
            basic_queue, mscalar_queue, nem12_queue,
            conn, debug=False, util_batch_size=10, jur_batch_size=5, tni_batch_size=10
    ):
        DBLambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table, conn=conn, debug=debug, failure_queue=failure_queue
        )
        self.ddb_meterdata_table = ddb_meterdata_table
        self.data_queue = Queue(queue=data_queue, debug=debug, log=self.log)
        self.segment_queue = Queue(queue=segment_queue, debug=debug, log=self.log)
        self.export_queue = Queue(queue=export_queue, debug=debug, log=self.log)
        self.basic_queue = Queue(queue=basic_queue, debug=debug, log=self.log)
        self.mscalar_queue = Queue(queue=mscalar_queue, debug=debug, log=self.log)
        self.nem12_queue = Queue(queue=nem12_queue, debug=debug, log=self.log)
        self.util_batch_size = util_batch_size
        self.jur_batch_size = jur_batch_size
        self.tni_batch_size = tni_batch_size
        self._handler = {
            'Utility': self._initiate_site_data_processing,
            'Validate': self._initiate_site_data_processing,
            'SegmentJurisdiction': self._initiate_segment_jurisdiction_calculation,
            'SegmentTNI': self._initiate_segment_tni_calculation,
            'SegmentJurisdictionCheck': self._initiate_segment_jurisdiction_check,
            'SegmentJurisdictionFix': self._initiate_segment_jurisdiction_fix,
            'Requeue': self._initiate_requeue,
            'NEM12': self._initiate_nem12_files,
            'MicroScalar': self._initiate_microscalar,
            'BasicRead': self._initiate_basic_read,
            'ResetBasicRead': self._initiate_reset_basic_read,
            'ExportTables': self._initiate_export_tables
        }

    def _initiate_requeue(self, _run_type, body, _debug):
        site = SiteRequeue(sqs_client=self.sqs_client)
        site.initiate_requeue(
            source_queue_name=body['source_queue'],
            target_queue_name=body['target_queue']
        )

    def _initiate_segment_tni_calculation(self, run_type, body, debug):
        run_date: str = body['run_date']
        source_date: str = body['source_date']
        identifier_type: str = body['identifier_type']
        site = SegmentTNI(
            queue=self.segment_queue, conn=self.conn, log=self.log, debug=debug,
            ddb_meterdata_table=self.ddb_meterdata_table,
            batch_size=self.tni_batch_size
        )
        site.initiate_segment_tni_calculation(
            run_type=run_type,
            run_date=run_date,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            source_date=source_date,
            identifier_type=identifier_type
        )

    def _initiate_segment_jurisdiction_calculation(self, run_type, body, debug):
        run_date: str = body['run_date']
        source_date: str = body['source_date']
        identifier_type: str = body['identifier_type']
        site = SegmentJurisdiction(
            queue=self.segment_queue, conn=self.conn, log=self.log, debug=debug,
            ddb_meterdata_table=self.ddb_meterdata_table,
            batch_size=self.jur_batch_size
        )
        site.initiate_segment_jurisdiction_calculation(
            run_type=run_type,
            run_date=run_date,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            source_date=source_date,
            identifier_type=identifier_type
        )

    def _initiate_segment_jurisdiction_check(self, run_type, body, debug):
        run_date: str = body['run_date']
        today: str = body['today']
        identifier_type = body['identifier_type']
        site = Check(
            queue=self.segment_queue, conn=self.conn, log=self.log, debug=debug
        )
        site.initiate_segment_jurisdiction_check(
            run_type=run_type,
            run_date=run_date,
            today=today,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            identifier_type=identifier_type
        )

    def _initiate_segment_jurisdiction_fix(self, run_type, body, debug):
        run_date: str = body['run_date']
        today: str = body['today']
        identifier_type = body['identifier_type']
        site = Fix(
            queue=self.segment_queue, conn=self.conn, log=self.log, debug=debug
        )
        site.initiate_segment_jurisdiction_fix(
            run_type=run_type,
            run_date=run_date,
            today=today,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            identifier_type=identifier_type
        )

    def _initiate_site_data_processing(self, run_type, body, debug):
        run_date: str = body['run_date']
        today: str = body['today']
        source_date: str = body['source_date']
        site = SiteData(
            queue=self.data_queue, conn=self.conn, log=self.log, debug=debug,
            ddb_meterdata_table=self.ddb_meterdata_table,
            batch_size=self.util_batch_size
        )
        limit = body.get('limit', 1000000000)
        site.initiate_site_data_processing(
            run_type=run_type,
            run_date=run_date,
            today=today,
            start_inclusive=body['start_inclusive'],
            end_exclusive=body['end_exclusive'],
            source_date=source_date,
            limit=limit
        )

    def _initiate_nem12_files(self, run_type, body, debug):
        today: str = body['today']
        run_date: str = body['run_date']
        action = NEM12(
            queue=self.nem12_queue, conn=self.conn, log=self.log, debug=debug
        )
        identifier_type = body['identifier_type']
        start_inclusive = body.get('start_inclusive')
        end_exclusive = body.get('end_exclusive')
        action.initiate_nem12_file_generation(
            run_type=run_type,
            today=today,
            run_date=run_date,
            start_inclusive=start_inclusive,
            end_exclusive=end_exclusive,
            identifier_type=identifier_type
        )

    def _initiate_microscalar(self, run_type, body, debug):
        today: str = body['today']
        run_date: str = body['run_date']
        action = MicroScalar(
            queue=self.mscalar_queue, conn=self.conn, log=self.log, debug=debug
        )
        identifier_type = body['identifier_type']
        action.initiate_microscalar_calculation(
            run_type=run_type,
            today=today,
            run_date=run_date,
            identifier_type=identifier_type
        )

    def _initiate_basic_read(self, run_type, body, debug):
        today: str = body['today']
        run_date: str = body['run_date']
        action = BasicRead(
            queue=self.basic_queue, conn=self.conn, log=self.log, debug=debug
        )
        identifier_type = body['identifier_type']
        action.initiate_basic_read_calculation(
            run_type=run_type,
            today=today,
            run_date=run_date,
            identifier_type=identifier_type
        )

    def _initiate_reset_basic_read(self, run_type, body, debug):
        today: str = body['today']
        run_date: str = body['run_date']
        action = BasicRead(
            queue=self.basic_queue, conn=self.conn, log=self.log, debug=debug
        )
        identifier_type = body['identifier_type']
        action.initiate_reset_basic_read_calculation(
            run_type=run_type,
            today=today,
            run_date=run_date,
            identifier_type=identifier_type
        )

    def _initiate_export_tables(self, run_type, body, debug):
        run_date: str = body['run_date']
        today: str = body['today']
        action = Exporter(
            queue=self.export_queue, conn=self.conn, log=self.log, debug=debug
        )
        table_names = body['table_names']
        partitions = body.get('partitions')
        file_format = body.get('file_format', 'csv')
        batch_size = body.get('batch_size', 1000000)
        bucket_prefix = body.get('bucket_prefix', 'export')
        action.initiate_export_tables(
            table_names=table_names,
            partitions=partitions,
            batch_size=batch_size,
            bucket_prefix=bucket_prefix,
            run_date=run_date,
            today=today,
            run_type=run_type,
            file_format=file_format
        )
