import json
import traceback
from typing import Dict, Callable, Optional, Union, Any

from bua.facade.connection import DBProxy
from bua.facade.sqs import SQS, Queue
from bua.site.handler import STATUS_DONE, STATUS_FAIL


class LambdaHandler:
    """
    Standard lambda handler base class.

    Override the _process_message(event) method in subclasses.
    """
    def __init__(self, sqs_client, ddb_table, debug, failure_queue, max_receive_count=10):
        self.sqs_client = sqs_client
        self.sqs = SQS(sqs_client=sqs_client, ddb_table=ddb_table)
        self.failure_queue = Queue(queue=failure_queue, debug=debug, log=self.log)
        self.debug = debug
        self._handler: Dict[str, Callable[[str, Dict, bool], Any]] = dict()
        self._default_handler: Optional[Callable[[Union[Dict, str]], None]] = None
        self.max_receive_count = max_receive_count

    def handle_request(self, event: Dict):
        self.log(event)
        if 'Records' in event:
            for record in event['Records']:
                if record['eventSource'] == 'aws:sqs':
                    if self.sqs.deduplicate_request(record):
                        body = record['body']
                        self.log(body)
                        try:
                            body = json.loads(record['body'])
                        except Exception as ex:
                            self.log(str(ex))
                        try:
                            self._process_message(body)
                        except Exception as ex:
                            self.log('Failed to process request')
                            traceback.print_exception(ex)
                            if self._handle_too_many_retries(record, ex):
                                return
                            if self.sqs.undo_deduplicate_request(record):
                                raise ex
                            self.send_failure(record['body'], str(ex))
        else:
            self._process_message(event)

    def send_failure(self, body: str, failure: str):
        self.failure_queue.send_request([
            {
                'body': body,
                'failure': failure
            }
        ])

    def _handle_too_many_retries(self, record, ex):
        if self.max_receive_count >= 0:
            if 'attributes' in record and 'ApproximateReceiveCount' in record['attributes']:
                receive_count = int(record['attributes']['ApproximateReceiveCount'])
                if receive_count >= self.max_receive_count:
                    self.log('Too many retries. Sending to the failure queue.')
                    self.send_failure(record['body'], f'Too many retries: {ex}')
                    return True
        return False

    def _process_message(self, body: Union[Dict, str]):
        self.log(body)
        if isinstance(body, dict):
            debug = self.debug or body.get('debug') is True
            if 'run_type' in body:
                self._process_with_run_type(body, debug)
                return
            if 'entries' in body:
                self._process_with_entries(body, debug)
                return
        if self._default_handler is not None:
            self._default_handler(body)

    def _process_with_entries(self, body: Dict, debug: bool):
        failure_entries = []
        failures = []
        db = body.get('db')
        if db is not None:
            self.log(db)
        for entry in body['entries']:
            self.log(entry)
            if 'db' not in entry and db is not None:
                entry['db'] = db
            if 'run_type' in entry:
                run_type: str = entry['run_type']
                if run_type in self._handler:
                    try:
                        handler = self._handler[run_type]
                        result = self._handle(handler, run_type, entry, debug)
                        if isinstance(result, dict) and 'status' in result and result['status'] != STATUS_DONE:
                            failure_entries.append(entry)
                            failures.append(result)
                    except KeyError as ex:
                        traceback.print_exception(ex)
                        result = {
                            'status': STATUS_FAIL,
                            'cause': str(ex)
                        }
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

    def _process_with_run_type(self, body: Dict, debug: bool):
        run_type: str = body['run_type']
        if run_type in self._handler:
            try:
                handler = self._handler[run_type]
                self._handle(handler, run_type, body, debug)
            except KeyError as ex:
                traceback.print_exception(ex)
                self.failure_queue.send_failure_event(body, str(ex))
        else:
            cause = f'Do not know how to handle run_type {run_type}'
            self.log(cause)
            self.failure_queue.send_failure_event(body, cause)

    @staticmethod
    def log(*args, **kwargs):
        print(*args, **kwargs)

    def _handle(self, handler: Callable[[str, Dict, bool], Any], run_type: str, body: Dict, debug: bool):
        return handler(run_type, body, debug)


class DBLambdaHandler(LambdaHandler):
    """
    Standard database lambda handler base class.

    Override the _process_message(event) and _initialise_connection() methods in subclasses.
    """

    def __init__(self, sqs_client, ddb_table, conn: DBProxy, ctl_conn: DBProxy, debug, failure_queue, max_receive_count=10):
        LambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_table, debug=debug, failure_queue=failure_queue,
            max_receive_count=max_receive_count
        )
        self.conn = conn
        self.ctl_conn = ctl_conn

    def connect(self, event: Dict):
        self.conn.connect(event)
        self.ctl_conn.connect(event)

    def _handle(self, handler: Callable[[str, Dict, bool], Any], run_type: str, event: Dict, debug: bool):
        self.connect(event['db'])
        with self.conn:
            with self.ctl_conn:
                return LambdaHandler._handle(self, handler, run_type, event, debug)
