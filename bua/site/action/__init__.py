import json
import traceback
from typing import List

from pymysql import Connection


class SQSAction:
    def __init__(self, queue, debug=False):
        self.queue = queue
        self.debug = debug

    def log(self, *args, **kwargs):
        print(*args, **kwargs)

    def send_if_needed(self, bodies: list, force=False, batch_size=10):
        """Send SQS message batches if needed"""
        if len(bodies) >= (batch_size*10) or (len(bodies) > 0 and force):
            batches = [
                {'entries': [bodies[n] for n in range(index, min(index+batch_size, len(bodies)))]}
                for index in range(0, len(bodies), batch_size)
            ]
            for index in range(0, len(batches), 10):
                self.send_request(batches[index:index+10])
            bodies.clear()

    def send_request(self, bodies: List):
        """Send an SQS message batch"""
        entries = [{'Id': str(index), 'MessageBody': json.dumps(body)} for index, body in enumerate(bodies)]
        response = self.queue.send_messages(Entries=entries)
        if self.debug:
            if 'Successful' in response:
                self.log(f'Sent {len(response["Successful"])} messages')
        while 'Failed' in response and len(response['Failed']) > 0:
            for failure in response['Failed']:
                self.log(f'Failed {failure["Id"]} : '
                      f'Sender fault {failure["SenderFault"]} : {failure["Code"]} : {failure["Message"]}')
            failures = {entry['Id'] for entry in response['Failed']}
            entries = [entry for entry in entries if entry['Id'] in failures]
            response = self.queue.send_messages(Entries=entries)
            if self.debug:
                if 'Successful' in response:
                    self.log(f'Sent {len(response["Successful"])} messages')


class Action(SQSAction):

    def __init__(self, queue, conn: Connection, debug=False):
        SQSAction.__init__(self, queue, debug)
        self.conn = conn

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
