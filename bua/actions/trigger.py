import os
import pathlib

import yaml

from bua.sqs import SQS


class Trigger:
    def __init__(self, config, sqs: SQS):
        self.sqs = sqs
        self.next_queue_url = config['next_queue_url']

    def trigger_restore(self, step, data):
        template_path = pathlib.Path(__file__).parent / 'restore_database.yml'
        if not os.path.exists(template_path):
            msg = f'Cannot find template {template_path}'
            return "ABORT", msg
        with open(template_path, 'r') as fp:
            pipeline = yaml.load(fp, yaml.Loader)
            if 'this' in data:
                pipeline['this'] = data['this']
            for key in {'suffix', 'update_id', 'snapshot_arn', 'run_date', 'today'}:
                if key in data:
                    pipeline['data'][key] = data[key]
            msg = yaml.dump(pipeline, yaml.Dumper)
        self.sqs.send_message(self.next_queue_url, msg, delay=60)
