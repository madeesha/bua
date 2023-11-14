from typing import Dict

from bua.facade.sqs import SQS
from bua.pipeline.handler.request import HandlerRequest


class Profiles:
    def __init__(self, config: Dict, sqs: SQS):
        self.config = config
        self.sqs = sqs
        self.prefix = self.config['prefix']

    def wait_for_empty_site_queues(self, request: HandlerRequest):
        data = request.data
        queue_depth: Dict[str, int] = data.get('queue_depth', {})
        args = request.step.get('args', dict())
        max_errors = int(args.get('max_errors', 0))
        status = 'COMPLETE'
        reason = 'None of the queues have messages'
        queues = self.sqs.describe_queues(queue_name_prefix=f'{self.prefix}-sqs-bua-site-')
        status, reason = self._check_for_messages_to_process(queues, status, reason)
        if status == 'COMPLETE':
            status, reason = self._check_for_message_failures(queues, status, reason, max_errors, queue_depth)
        return status, reason

    @staticmethod
    def _check_for_message_failures(
            queues: Dict, status: str, reason: str, max_errors: int, queue_depth: Dict[str, int]
    ):
        for queue_url, attributes in queues.items():
            queue_name = queue_url.split('/')[-1]
            total = attributes['Total']
            delta_errors = total - queue_depth.get(queue_name, 0)
            if delta_errors > 0:
                if queue_name.endswith('-failure-queue'):
                    if delta_errors > max_errors:
                        status = 'FAILED'
                        reason = f'Queue {queue_name} has about {total} messages of which {delta_errors} are new'
                        break
                    else:
                        continue
                if queue_name.endswith('-dlqueue'):
                    if delta_errors > max_errors:
                        status = 'FAILED'
                        reason = f'Queue {queue_name} has about {total} messages of which {delta_errors} are new'
                        break
                    else:
                        continue
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
        count_queues = 0
        count_messages = 0
        for queue_url, attributes in queues.items():
            queue_name = queue_url.split('/')[-1]
            total = attributes['Total']
            if total > 0:
                if queue_name.endswith('-failure-queue'):
                    print(f'Purge messages from {queue_name}')
                    self.sqs.empty_queue(queue_url)
                    count_queues += 1
                    count_messages += total
                elif queue_name.endswith('-dlqueue'):
                    print(f'Purge messages from {queue_name}')
                    self.sqs.empty_queue(queue_url)
                    count_queues += 1
                    count_messages += total
        return "COMPLETE", f'Purged {count_messages} messages from {count_queues} queues'

    def record_site_errors_queues(self, request: HandlerRequest):

        data = request.data
        queue_depth = data.get('queue_depth', {})
        data['queue_depth'] = queue_depth

        count_queues = 0
        queues = self.sqs.describe_queues(queue_name_prefix=f'{self.prefix}-sqs-bua-site-')
        for queue_url, attributes in queues.items():
            queue_name = queue_url.split('/')[-1]
            total = attributes['Total']
            if total > 0:
                if queue_name.endswith('-failure-queue'):
                    data['queue_depth'][queue_name] = total
                    count_queues += 1
                elif queue_name.endswith('-dlqueue'):
                    data['queue_depth'][queue_name] = total
                    count_queues += 1
        return "COMPLETE", f'Recorded {count_queues} error queue depths'
