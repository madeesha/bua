import json
import uuid
from typing import Union, Dict

from bua.handler import LambdaHandler


class BUANotifyHandler(LambdaHandler):

    def __init__(self, *, config, sqs_client, ddb_bua_table, failure_queue, sfn_client, debug=False):
        LambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table, debug=debug, failure_queue=failure_queue
        )
        self.config = config
        self.sfn_client = sfn_client
        self._default_handler = self._handle_event

    def _handle_event(self, body: Union[Dict, str]):
        event = {
            'steps': 'DoNothing',
            'snapshot_arn': body
        }
        self.sfn_client.start_execution(
            stateMachineArn=self.config['state_machine_arn'],
            name=uuid.uuid4().hex,
            input=json.dumps(event)
        )
