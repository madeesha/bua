import os
import pathlib
import yaml

from bua.pipeline.facade.sqs import SQS
from bua.pipeline.handler.request import HandlerRequest


class Trigger:
    def __init__(self, config, sqs: SQS):
        self.sqs = sqs
        self.next_queue_url = config['next_queue_url']

    def trigger_restore(self, request: HandlerRequest):
        data = request.data
        template_name = data.get('template_name', 'bua_restore')
        template_path = pathlib.Path(__file__).parent / f'{template_name}.yml'
        if not os.path.exists(template_path):
            msg = f'Cannot find template {template_path}'
            return "ABORT", msg
        with open(template_path, 'r') as fp:
            pipeline = yaml.load(fp, Loader=yaml.Loader)
            for key in {'instance', 'this', 'workflow_instance_id'}:
                if key in data:
                    pipeline[key] = data[key]
            for key in {'suffix', 'update_id', 'snapshot_arn', 'run_date', 'today'}:
                if key in data:
                    pipeline['data'][key] = data[key]
            msg = yaml.dump(pipeline, Dumper=yaml.Dumper)
        self.sqs.send_message(self.next_queue_url, msg, delay=60)
        return "COMPLETE", f"Triggered restore"
