from typing import Dict

from bua.facade.sqs import SQS
from bua.pipeline.handler.request import HandlerRequest


class Profiles:
    def __init__(self, config: Dict, sqs: SQS):
        self.config = config
        self.sqs = sqs
        self.prefix = self.config['prefix']

    def wait_for_empty_site_queues(self, _request: HandlerRequest):
        status = 'COMPLETE'
        reason = 'None of the queues have messages'
        queues = self.sqs.describe_queues(queue_name_prefix=f'{self.prefix}-sqs-bua-site-')
        status, reason = self._check_for_messages_to_process(queues, status, reason)
        if status == 'COMPLETE':
            status, reason = self._check_for_message_failures(queues, status, reason)
        return status, reason

    def _check_for_message_failures(self, queues, status, reason):
        for queue_url, attributes in queues.items():
            queue_name = queue_url.split('/')[-1]
            total = attributes['Total']
            if total > 0:
                if queue_name.endswith('-failure-queue'):
                    status = 'FAILED'
                    reason = f'Queue {queue_name} has about {total} messages'
                    break
                if queue_name.endswith('-dlqueue'):
                    status = 'FAILED'
                    reason = f'Queue {queue_name} has about {total} messages'
                    break
                status = 'RETRY'
                reason = f'Queue {queue_name} has about {total} messages'
                break
        return status, reason

    @staticmethod
    def _check_for_messages_to_process(queues, status, reason):
        for queue_url, attributes in queues.items():
            queue_name = queue_url.split('/')[-1]
            if queue_name.endswith('-failure-queue'):
                continue
            if queue_name.endswith('-dlqueue'):
                continue
            total = attributes['Total']
            if total > 0:
                status = 'RETRY'
                reason = f'Queue {queue_name} has about {total} messages'
                break
        return status, reason

    def empty_site_errors_queues(self, _request: HandlerRequest):
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
