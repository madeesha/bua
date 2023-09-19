from typing import Dict

from bua.pipeline.facade.sqs import SQS


class Profiles:
    def __init__(self, config: Dict, sqs: SQS):
        self.config = config
        self.sqs = sqs
        self.prefix = self.config['prefix']

    def wait_for_empty_site_queues(self, step, data):
        queues = self.sqs.describe_queues(queue_name_prefix=f'{self.prefix}-sqs-bua-site-')
        for queue_url, attributes in queues.items():
            queue_name = queue_url.split('/')[-1]
            total = attributes['Total']
            if total > 0:
                if queue_name.endswith('-failure-queue'):
                    return "FAILED", f'Queue {queue_name} has about {total} messages'
                if queue_name.endswith('-dlqueue'):
                    return "FAILED", f'Queue {queue_name} has about {total} messages'
                return "RETRY", f'Queue {queue_name} has about {total} messages'
        return "COMPLETE", 'None of the queues have messages'

    def empty_site_errors_queues(self, step, data):
        queues = self.sqs.describe_queues(queue_name_prefix=f'{self.prefix}-sqs-bua-site-')
        for queue_url, attributes in queues.items():
            queue_name = queue_url.split('/')[-1]
            total = attributes['Total']
            if total > 0:
                if queue_name.endswith('-failure-queue'):
                    print(f'Purge messages from {queue_name}')
                    self.sqs.empty_queue(queue_url)
                elif queue_name.endswith('-dlqueue'):
                    print(f'Purge messages from {queue_name}')
                    self.sqs.empty_queue(queue_url)
        return "COMPLETE", 'Purged any error messages'
