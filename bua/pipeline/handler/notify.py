import json
import uuid
from typing import Union, Dict
from datetime import datetime
from zoneinfo import ZoneInfo

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
        run_date = datetime.now(ZoneInfo('Australia/Sydney')).strftime('%Y-%m-%d')
        unique_id = uuid.uuid4().hex
        id_part_1 = unique_id[0:8]
        id_part_2 = unique_id[8:12]
        id_part_3 = unique_id[12:16]
        id_part_4 = unique_id[16:20]
        id_part_5 = unique_id[20:]
        step_execution_name = f"{run_date}-Notify-{id_part_1}-{id_part_2}-{id_part_3}-{id_part_4}-{id_part_5}"
        self.sfn_client.start_execution(
            stateMachineArn=self.config['state_machine_arn'],
            name=step_execution_name,
            input=json.dumps(event)
        )
