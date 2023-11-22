import uuid
from typing import Dict, Optional, List


class MySQSQueue:
    """Local SQS queue implementation for testing"""

    def __init__(self):
        self.visible_message_ids: List[str] = []
        self.invisible_message_ids: List[str] = []
        self.messages: Dict[str, str] = {}

    def put_message(self, message: str) -> str:
        message_id = uuid.uuid4().hex
        self.messages[message_id] = message
        self.visible_message_ids.append(message_id)
        return message_id

    def get_message(self) -> Optional[Dict[str, str]]:
        try:
            message_id = self.visible_message_ids.pop(0)
            self.invisible_message_ids.append(message_id)
            message = self.messages[message_id]
            return {
                'message_id': message_id,
                'message': message
            }
        except IndexError:
            return None

    def delete_message(self, message_id) -> None:
        if message_id in self.invisible_message_ids:
            self.invisible_message_ids.remove(message_id)
        if message_id in self.messages:
            del self.messages[message_id]


class MySQS:
    """Local SQS implementation for testing"""

    def __init__(self):
        self.queues: Dict[str, MySQSQueue] = {}

    def get_queue(self, queue_name: str) -> MySQSQueue:
        queue = self.queues.get(queue_name)
        if queue is None:
            queue = MySQSQueue()
            self.queues[queue_name] = queue
        return queue

    def patch(self):
        self.queues.clear()


class MonkeyPatchSQSResource:
    """Pretend to be a boto3 SQS resource"""

    def __init__(self, mysqs: MySQS):
        self.mysqs = mysqs

    def Queue(self, url):
        queue_name = url.split('/')[-1]
        return MonkeyPatchQueue(queue_name, self.mysqs)

    def patch(self):
        pass


class MonkeyPatchSQSClient:
    """Pretend to be a boto3 SQS client object"""

    def __init__(self, mysqs: MySQS):
        self.mysqs = mysqs

    def patch(self):
        pass

    def send_message(self, **kwargs):
        queue_url = kwargs['QueueUrl']
        queue_name = queue_url.split('/')[-1]
        message_body = kwargs['MessageBody']
        queue = self.mysqs.get_queue(queue_name)
        message_id = queue.put_message(message_body)
        return {
            'MessageId': message_id
        }

    def send_message_batch(self, **kwargs):
        queue_url = kwargs['QueueUrl']
        queue_name = queue_url.split('/')[-1]
        queue = self.mysqs.get_queue(queue_name)
        entries = kwargs['Entries']
        results = {
            'Successful': [],
            'Failed': [],
        }
        for entry in entries:
            message_id = queue.put_message(entry['MessageBody'])
            results['Successful'].append({
                'Id': entry['Id'],
                'MessageId': message_id,
            })
        return results

    def delete_message_batch(self, **kwargs):
        queue_url = kwargs['QueueUrl']
        queue_name = queue_url.split('/')[-1]
        queue = self.mysqs.get_queue(queue_name)
        entries = kwargs['Entries']
        results = {
            'Successful': [],
            'Failed': [],
        }
        for entry in entries:
            queue.delete_message(entry['ReceiptHandle'])
            results['Successful'].append({
                'Id': entry['Id'],
            })
        return results

    def receive_message(self, **kwargs):
        queue_url = kwargs['QueueUrl']
        max_number_of_messages = int(kwargs.get('MaxNumberOfMessages', 10))
        queue_name = queue_url.split('/')[-1]
        queue = self.mysqs.get_queue(queue_name)
        messages = []
        message = queue.get_message()
        while message is not None and len(messages) < max_number_of_messages:
            messages.append({
                'MessageId': message['message_id'],
                'ReceiptHandle': message['message_id'],
                'Body': message['message']
            })
            message = queue.get_message()
        return {
            'Messages': messages
        }


class MonkeyPatchQueue:
    """Pretend to be a boto3 Queue object"""

    def __init__(self, queue_name: str, mysqs: MySQS):
        self.queue_name = queue_name
        self.mysqs = mysqs

    def patch(self):
        pass

    def send_message(self, **kwargs):
        message_body = kwargs['MessageBody']
        queue = self.mysqs.get_queue(self.queue_name)
        message_id = queue.put_message(message_body)
        return {
            'MessageId': message_id
        }

    def send_messages(self, **kwargs):
        entries = kwargs['Entries']
        queue = self.mysqs.get_queue(self.queue_name)
        successful = []
        for entry in entries:
            id = entry['Id']
            message_body = entry['MessageBody']
            message_id = queue.put_message(message_body)
            successful.append({
                'Id': id,
                'MessageId': message_id
            })
        return {
            'Successful': successful
        }
