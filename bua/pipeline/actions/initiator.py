import json
from typing import Dict

from bua.facade.sqs import SQS
from bua.pipeline.handler.request import HandlerRequest


class Initiator:

    def __init__(self, config: Dict, sqs: SQS):
        self.sqs = sqs
        self.queue_url = config['initiate_queue_url']
        self.prefix = config['prefix']

    def bua_initiate(self, request: HandlerRequest):
        data = request.data
        run_type = data['run_type']
        start_inclusive = data['start_inclusive']
        end_exclusive = data['end_exclusive']
        end_inclusive = data['end_inclusive']
        today = data['today']
        run_date = data['run_date']
        current_date = data['current_date']
        current_time = data['current_time']
        source_date = data.get('source_date')
        identifier_type = data.get('identifier_type')
        update_id = data['update_id']
        suffix = data['suffix']
        domain = data['domain']
        schema = data['schema']
        other_args = request.step.get('args', dict())
        message = {
            'run_type': run_type,
            'run_date': run_date,
            'today': today,
            'start_inclusive': start_inclusive,
            'end_exclusive': end_exclusive,
            'end_inclusive': end_inclusive,
            'source_date': source_date,
            'current_date': current_date,
            'current_time': current_time,
            'identifier_type': identifier_type,
            'db': {
                'prefix': self.prefix,
                'update_id': update_id,
                'suffix': suffix,
                'domain': domain,
                'schema': schema,
            }
        }
        for key in other_args.keys():
            message[key] = data[key]
        body = json.dumps(message)
        self.sqs.send_message(self.queue_url, body)
        return "COMPLETE", f'Initiated BUA {run_type} as at {today}'
