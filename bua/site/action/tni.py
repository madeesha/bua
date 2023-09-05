import traceback

from pymysql import Connection

from bua.site.action import Action


class SegmentTNI(Action):

    def __init__(self, table, queue, conn: Connection, debug=False, batch_size=10, check_nem=True, check_aggread=False):
        super().__init__(queue, conn, debug)
        self.table = table
        self.batch_size = batch_size
        self.check_nem = check_nem
        self.check_aggread = check_aggread

    def initiate_segment_tni_calculation(self, run_type, run_date, start_inclusive, end_exclusive, source_date, identifier_type):
        """Initiate the calculation of tni profile segments"""
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
                    "SELECT DISTINCT jurisdiction_name, tni_name, res_bus, stream_type, interval_date "
                    "FROM UtilityProfile "
                    "WHERE run_date = %s "
                    "AND identifier_type = 'Utility' "
                    "AND interval_date >= %s "
                    "AND interval_date < %s ",
                    (source_date, start_inclusive, end_exclusive)
                )
                total = 0
                for record in cur.fetchall_unbuffered():
                    jurisdiction_name = record['jurisdiction_name']
                    tni_name = record['tni_name']
                    res_bus = record['res_bus']
                    stream_type = record['stream_type']
                    interval_date = record['interval_date']
                    body = {
                        'run_type': run_type,
                        'run_date': run_date,
                        'identifier_type': identifier_type,
                        'jurisdiction_name': jurisdiction_name,
                        'tni_name': tni_name,
                        'res_bus': res_bus,
                        'stream_type': stream_type,
                        'interval_date': interval_date.strftime('%Y%m%d'),
                        'source_date': source_date,
                        'avg_sum': 'Average' if 'Avg' in identifier_type else 'Sum',
                        'incl_est': 'InclEst' in identifier_type
                    }
                    bodies.append(body)
                    self.send_if_needed(bodies, batch_size=self.batch_size)
                    total += 1
                self.send_if_needed(bodies, force=True, batch_size=self.batch_size)
                cur.execute(
                    "INSERT INTO UtilityProfileLog (run_date, run_type, source_date, total_entries) "
                    "VALUES (%s,%s,%s,%s)",
                    (run_date, identifier_type, source_date, total)
                )
                self.log(f'{total} tni profile segment calculations initiated')
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise
