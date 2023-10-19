import traceback
from typing import Callable

from pymysql import InternalError, InterfaceError

from bua.facade.connection import DB
from bua.facade.sqs import Queue
from bua.site.action import Action
from bua.site.handler import STATUS_DONE


class SiteSegment(Action):
    """Management of utility profile site data including profile segment calculations"""

    def __init__(
            self, queue: Queue, conn: DB, log: Callable, debug: bool,
            meterdata_table, batch_size=10, check_nem=True, check_aggread=False
    ):
        Action.__init__(self, queue, conn, log, debug)
        self.meterdata_table = meterdata_table
        self.batch_size = batch_size
        self.check_nem = check_nem
        self.check_aggread = check_aggread

    def calculate_profile_segment(self, identifier_type, run_date, source_date, jurisdiction_name, res_bus, stream_type,
                                  interval_date,
                                  tni_name=None, avg_sum='Average', incl_est=True):
        """Calculate a profile segment"""
        with self.conn.cursor() as cur:
            try:
                by_tni = tni_name is not None
                if by_tni:
                    identifier = f'{jurisdiction_name}|{tni_name}|{res_bus}|{stream_type}'
                else:
                    identifier = f'{jurisdiction_name}|{res_bus}|{stream_type}'
                SiteSegment._cleanup_existing_segment_records(cur, run_date, identifier_type, identifier, interval_date)
                sql = """
                INSERT INTO UtilityProfileSegment
                ( run_date, identifier_type, identifier, jurisdiction_name, 
                  res_bus, stream_type, interval_date, tni_name
                """
                for index in range(1, 49):
                    sql += f"""
                    , value_{index:02d}, count_{index:02d}, quality_{index:02d}
                    """
                if by_tni:
                    sql += f"""
                    )
                    SELECT
                    %s AS run_date,
                    %s AS identifier_type,
                    %s AS identifier,
                    jurisdiction_name, res_bus, stream_type,
                    interval_date, tni_name
                    """
                else:
                    sql += f"""
                    )
                    SELECT
                    %s AS run_date,
                    %s AS identifier_type,
                    %s AS identifier,
                    jurisdiction_name, res_bus, stream_type,
                    interval_date, ''
                    """
                for index in range(1, 49):
                    if avg_sum == 'Average':
                        if incl_est:
                            sql += f"""
                            , ROUND(AVG(value_{index:02d}),6) AS value_{index:02d}
                            , COUNT(value_{index:02d})
                            , SUBSTR(MIN(CASE SUBSTR(quality_{index:02d},1,1) 
                                WHEN 'E' THEN CONCAT('1',quality_{index:02d}) 
                                WHEN 'S' THEN CONCAT('2',quality_{index:02d}) 
                                WHEN 'A' THEN CONCAT('3',quality_{index:02d}) 
                                ELSE CONCAT('4',quality_{index:02d}) END),2) AS quality_{index:02d}
                            """
                        else:
                            sql += f"""
                            , ROUND(AVG(IF(SUBSTR(quality_{index:02d},1,1)='E',
                                0,
                                value_{index:02d})),6) AS value_{index:02d}
                            , SUM(IF(SUBSTR(quality_{index:02d},1,1)='E',0,1))
                            , SUBSTR(MIN(CASE SUBSTR(quality_{index:02d},1,1) 
                                WHEN 'S' THEN CONCAT('2',quality_{index:02d}) 
                                WHEN 'A' THEN CONCAT('3',quality_{index:02d}) 
                                WHEN 'F' THEN CONCAT('4',quality_{index:02d}) 
                                ELSE CONCAT('5',quality_{index:02d}) END),2) AS quality_{index:02d}
                            """
                    else:
                        if incl_est:
                            sql += f"""
                            , SUM(value_{index:02d}) AS value_{index:02d}
                            , COUNT(value_{index:02d})
                            , SUBSTR(MIN(CASE SUBSTR(quality_{index:02d},1,1) 
                                WHEN 'E' THEN CONCAT('1',quality_{index:02d}) 
                                WHEN 'S' THEN CONCAT('2',quality_{index:02d}) 
                                WHEN 'A' THEN CONCAT('3',quality_{index:02d}) 
                                ELSE CONCAT('4',quality_{index:02d}) END),2) AS quality_{index:02d}
                            """
                        else:
                            sql += f"""
                            , SUM(IF(SUBSTR(quality_{index:02d},1,1)='E',0,value_{index:02d})) AS value_{index:02d}
                            , SUM(IF(SUBSTR(quality_{index:02d},1,1)='E',0,1))
                            , SUBSTR(MIN(CASE SUBSTR(quality_{index:02d},1,1) 
                                WHEN 'S' THEN CONCAT('2',quality_{index:02d}) 
                                WHEN 'A' THEN CONCAT('3',quality_{index:02d}) 
                                WHEN 'F' THEN CONCAT('4',quality_{index:02d}) 
                                ELSE CONCAT('5',quality_{index:02d}) END),2) AS quality_{index:02d}
                            """
                sql += """
                FROM UtilityProfile
                WHERE run_date = %s
                AND identifier_type = 'Utility'
                AND jurisdiction_name = %s
                """
                if by_tni:
                    sql += """
                    AND tni_name = %s
                    """
                sql += """
                AND res_bus = %s
                AND stream_type = %s
                AND interval_date = %s
                AND INSTR('EB',SUBSTR(nmi_suffix,1,1)) > 0
                AND NOT EXISTS (SELECT * FROM UtilityProfileExclusion 
                                WHERE identifier = UtilityProfile.identifier 
                                AND identifier_type = %s
                                AND run_date = %s)
                """
                if by_tni:
                    sql += """
                    GROUP BY jurisdiction_name, tni_name, res_bus, stream_type, interval_date
                    """
                else:
                    sql += """
                    GROUP BY jurisdiction_name, res_bus, stream_type, interval_date
                    """
                if by_tni:
                    params = (
                        run_date, identifier_type, identifier, source_date, jurisdiction_name, tni_name,
                        res_bus, stream_type, interval_date, identifier_type, run_date
                    )
                    cur.execute(sql, params)
                else:
                    params = (
                        run_date, identifier_type, identifier, source_date, jurisdiction_name,
                        res_bus, stream_type, interval_date, identifier_type, run_date
                    )
                    cur.execute(sql, params)
                rows_affected = cur.rowcount
                Action.record_processing_summary(
                    cur, run_date, identifier_type, identifier, interval_date, rows_affected
                )
                self.log('Imported', rows_affected, 'records for segment', identifier, 'on', interval_date)
                self.conn.commit()
                return {
                    'status': STATUS_DONE
                }
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

    @staticmethod
    def _cleanup_existing_segment_records(cur, run_date, run_type, identifier, interval_date):
        cur.execute(
            "DELETE FROM UtilityProfileSummary "
            "WHERE run_date = %s "
            "AND identifier_type = %s "
            "AND identifier = %s "
            "AND interval_date = %s",
            (run_date, run_type, identifier, interval_date)
        )
        cur.execute(
            "DELETE FROM UtilityProfileSegment "
            "WHERE run_date = %s "
            "AND identifier_type = %s "
            "AND identifier = %s "
            "AND interval_date = %s",
            (run_date, run_type, identifier, interval_date)
        )
