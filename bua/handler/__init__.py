import json
from typing import Dict, Callable

from bua.facade.sqs import SQS, Queue
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class LambdaHandler:
    """
    Standard lambda handler base class.

    Override the _process_message(event) method in subclasses.
    """
    def __init__(self, sqs_client, ddb_table, debug, failure_queue):
        self.sqs_client = sqs_client
        self.sqs = SQS(sqs_client=sqs_client, ddb_table=ddb_table)
        self.failure_queue = Queue(queue=failure_queue, debug=debug, log=self.log)
        self.debug = debug
        self._handler: Dict[str, Callable[[str, Dict, bool], None]] = dict()

    def handle_request(self, event: Dict):
        self.log(event)
        if 'Records' in event:
            for record in event['Records']:
                if record['eventSource'] == 'aws:sqs':
                    if self.sqs.deduplicate_request(record):
                        body = json.loads(record['body'])
                        self._process_message(body)
        else:
            self._process_message(event)

    def _process_message(self, body):
        debug = self.debug or body.get('debug') is True
        self.log(body)
        if 'run_type' in body:
            run_type: str = body['run_type']
            if run_type in self._handler:
                self._handler[run_type](run_type, body, debug)
            else:
                cause = f'Do not know how to handle run_type {run_type}'
                self.log(cause)
                self.failure_queue.send_failure_event(body, cause)
            return
        if 'entries' in body:
            failure_entries = []
            failures = []
            for entry in body['entries']:
                if 'run_type' in entry:
                    run_type: str = entry['run_type']
                    if run_type in self._handler:
                        result = self._handler[run_type](run_type, entry, debug)
                        if isinstance(result, dict) and 'status' in result and result['status'] != STATUS_DONE:
                            failure_entries.append(entry)
                            failures.append(result)
                    else:
                        cause = f'Do not know how to handle run_type {run_type}'
                        self.log(cause)
                        failure_entries.append(entry)
                        failures.append({
                            'status': STATUS_FAIL,
                            'cause': cause
                        })
            if len(failures) > 0:
                self.failure_queue.send_request([
                    {
                        'entries': failure_entries,
                        'failures': failures
                    }
                ])
            return

    @staticmethod
    def log(*args, **kwargs):
        print(*args, **kwargs)


class DBLambdaHandler(LambdaHandler):
    """
    Standard database lambda handler base class.

    Override the _process_message(event) and _initialise_connection() methods in subclasses.
    """

    def __init__(self, sqs_client, ddb_table, conn, debug, failure_queue, lock_wait_timeout=60):
        LambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_table, debug=debug, failure_queue=failure_queue
        )
        self.conn = conn
        self.lock_wait_timeout = lock_wait_timeout
        self._initialise_connection()

    def reconnect(self, conn):
        self.conn = conn
        self._initialise_connection()

    def _initialise_connection(self):
        with self.conn.cursor() as cur:
            cur.execute(f"SET SESSION innodb_lock_wait_timeout = {self.lock_wait_timeout}")
