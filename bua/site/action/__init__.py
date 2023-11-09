import traceback
from typing import Callable
from bua.facade.connection import DBProxy
from bua.facade.sqs import Queue


class Action:

    def __init__(self, queue: Queue, conn: DBProxy, log: Callable, debug: bool):
        self.queue = queue
        self.conn = conn
        self.log = log
        self.debug = debug

    def auto_exclude_nmis(self, run_date, identifier_type, source_date):
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT IGNORE INTO UtilityProfileExclusion
                    (run_date, identifier_type, identifier, exclusion_reason)
                    SELECT DISTINCT %s, %s, up.identifier, ud.utility_classification
                    FROM UtilityProfile up
                    JOIN Utility ut ON ut.identifier = up.identifier
                    JOIN UtilityDetail ud ON ud.utility_id = ut.id 
                    AND ud.utility_status = 'A' 
                    AND up.interval_date BETWEEN ud.start_date AND COALESCE(ud.end_date,NOW())
                    WHERE up.run_date = %s
                    AND up.identifier_type = 'Utility'
                    AND ud.utility_classification IN ('GENERATR', 'EPROFILE')
                    """,
                    (run_date, identifier_type, source_date)
                )
                self.conn.commit()
            except Exception as ex:
                traceback.print_exception(ex)
                self.conn.rollback()
                raise

    @staticmethod
    def record_processing_summary(cur, run_date, identifier_type, identifier, interval_date, rows_affected):
        cur.execute(
            "INSERT INTO UtilityProfileSummary "
            "(run_date, identifier_type, identifier, interval_date, total_records) "
            "VALUES (%s, %s, %s, %s, %s)",
            (run_date, identifier_type, identifier, interval_date, rows_affected)
        )
