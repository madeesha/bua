import traceback
from typing import Callable

from pymysql import InternalError, InterfaceError

from bua.facade.connection import DBProxy
from bua.facade.sqs import Queue
from bua.site.action import Action


class SegmentJurisdiction(Action):

    def __init__(
            self, queue: Queue, conn: DBProxy, log: Callable, debug: bool,
            ddb_meterdata_table, batch_size=10, check_nem=True, check_aggread=False
    ):
        Action.__init__(self, queue, conn, log, debug)
        self.table = ddb_meterdata_table
        self.batch_size = batch_size
        self.check_nem = check_nem
        self.check_aggread = check_aggread

    def initiate_segment_jurisdiction_calculation(
            self, run_type, run_date, start_inclusive, end_exclusive, source_date, identifier_type, db
    ):
        """Initiate the calculation of jurisdiction profile segments"""
        bodies = []
        self.auto_exclude_nmis(run_date, identifier_type, source_date)
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    "DELETE FROM UtilityProfileSummary "
                    "WHERE run_date = %s "
                    "AND identifier_type = %s",
                    (run_date, identifier_type)
                )
                cur.execute(
                    "DELETE FROM UtilityProfileSegment "
                    "WHERE run_date = %s "
                    "AND identifier_type = %s",
                    (run_date, identifier_type)
                )
                cur.execute(
                    "SELECT DISTINCT jurisdiction_name, res_bus, stream_type, interval_date "
                    "FROM UtilityProfile "
                    "WHERE run_date = %s "
                    "AND identifier_type = 'Utility' "
                    "AND interval_date >= %s "
                    "AND interval_date < %s ",
                    (source_date, start_inclusive, end_exclusive)
                )
                total = 0
                for record in cur.fetchall_unbuffered():
                    jurisdiction = record['jurisdiction_name']
                    res_bus = record['res_bus']
                    stream_type = record['stream_type']
                    interval_date = record['interval_date']
                    body = {
                        'run_type': run_type,
                        'run_date': run_date,
                        'identifier_type': identifier_type,
                        'jurisdiction_name': jurisdiction,
                        'res_bus': res_bus,
                        'stream_type': stream_type,
                        'interval_date': interval_date.strftime('%Y%m%d'),
                        'source_date': source_date,
                        'avg_sum': 'Average' if 'Avg' in identifier_type else 'Sum',
                        'incl_est': 'InclEst' in identifier_type,
                        'db': db,
                    }
                    bodies.append(body)
                    self.queue.send_if_needed(bodies, batch_size=self.batch_size)
                    total += 1
                self.queue.send_if_needed(bodies, force=True, batch_size=self.batch_size)
                cur.execute(
                    "INSERT INTO UtilityProfileLog (run_date, run_type, source_date, total_entries) "
                    "VALUES (%s,%s,%s,%s)",
                    (run_date, identifier_type, source_date, total)
                )
                self.log(f'{total} jurisdiction profile segment calculations initiated')
                self.conn.commit()
            except InternalError as ex:
                traceback.print_exception(ex)
                raise
            except InterfaceError as ex:
                traceback.print_exception(ex)
                raise
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
