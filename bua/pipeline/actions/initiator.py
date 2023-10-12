import json
from typing import Dict

from bua.facade.sqs import SQS
from bua.pipeline.handler.request import HandlerRequest


class Initiator:

    def __init__(self, config: Dict, sqs: SQS):
        self.sqs = sqs
        self.queue_url = config['initiate_queue_url']

    def bua_initiate(self, request: HandlerRequest):
        data = request.data
        run_type = data['run_type']
        start_inclusive = data.get('start_inclusive')
        end_exclusive = data.get('end_exclusive')
        today = data['today']
        run_date = data['run_date']
        source_date = data.get('source_date')
        identifier_type = data.get('identifier_type')
        other_args = request.step.get('args', dict())
        message = {
            'run_type': run_type,
            'run_date': run_date,
            'today': today,
            'start_inclusive': start_inclusive,
            'end_exclusive': end_exclusive,
            'source_date': source_date,
            'identifier_type': identifier_type,
        }
        for key in other_args.keys():
            message[key] = data[key]
        body = json.dumps(message)
        self.sqs.send_message(self.queue_url, body)
        return "COMPLETE", f'Initiated BUA {run_type} as at {today}'
