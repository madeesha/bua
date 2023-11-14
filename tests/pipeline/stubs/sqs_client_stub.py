import yaml


class SQSClientStub:

    def __init__(self, failure_queue_url):
        self.messages = []
        self.failure_queue_url = failure_queue_url
        failure_queue_name = failure_queue_url.split('/')[-1]
        self.get_queue_attributes_response = {
            failure_queue_name: {
                'Attributes': {
                    'ApproximateNumberOfMessages': 100,
                    'ApproximateNumberOfMessagesNotVisible': 100,
                    'ApproximateNumberOfMessagesDelayed': 100
                }
            }
        }

    def assert_no_failures(self):
        for message in self.messages:
            assert message['QueueUrl'] != self.failure_queue_url

    def assert_no_messages(self):
        assert len(self.messages) == 0

    def assert_retry_status(self):
        assert len(self.messages) > 0, 'No retry messages'
        for message in self.messages:
            body = yaml.load(message['MessageBody'], yaml.Loader)
            assert body['result']['status'] == 'RETRY', f'Unexpected {message}'

    def assert_complete_status(self):
        assert len(self.messages) > 0, 'No complete messages'
        for message in self.messages:
            body = yaml.load(message['MessageBody'], yaml.Loader)
            assert body['result']['status'] == 'COMPLETE', f'Unexpected {message}'

    def send_message(self, *_args, **kwargs):
        self.messages.append({**kwargs})
        for key in kwargs.keys():
            assert key in {'QueueUrl', 'MessageBody', 'DelaySeconds'}
        return {
            'MessageId': '123'
        }

    def delete_message(self, *_args, **kwargs):
        for key in kwargs.keys():
            assert key in {'QueueUrl', 'ReceiptHandle'}
        return {}

    def list_queues(self, **kwargs):
        for key in kwargs.keys():
            assert key in {'QueueNamePrefix', 'MaxResults'}
        queue_name_prefix = kwargs['QueueNamePrefix']
        return {
            'QueueUrls': [
                f'http://localhost/sqs/{name}' for name in self.get_queue_attributes_response.keys() if name.startswith(queue_name_prefix)
            ]
        }

    def get_queue_attributes(self, **kwargs):
        for key in kwargs.keys():
            assert key in {'QueueUrl', 'AttributeNames'}, f'Unexpected key {key}'
        queue_url = kwargs['QueueUrl']
        queue_name = queue_url.split('/')[-1]
        return self.get_queue_attributes_response[queue_name]

    def purge_queue(self, **kwargs):
        for key in kwargs.keys():
            assert key in {'QueueUrl',}, f'Unexpected key {key}'
        queue_url = kwargs['QueueUrl']
        queue_name = queue_url.split('/')[-1]
        if queue_name in self.get_queue_attributes_response:
            self.get_queue_attributes_response[queue_name] = {
                'Attributes': {
                    'ApproximateNumberOfMessages': 0,
                    'ApproximateNumberOfMessagesNotVisible': 0,
                    'ApproximateNumberOfMessagesDelayed': 0
                }
            }
        else:
            raise ValueError(f'{queue_name} is not found')


class SQSQueueStub:

    def __init__(self):
        self.messages = []

    def assert_no_messages(self):
        assert self.messages == []

    def send_message(self, *_args, **kwargs):
        self.messages.append({**kwargs})
        for key in kwargs.keys():
            assert key in {'MessageBody', 'DelaySeconds'}
        return {
            'MessageId': '123'
        }

    def send_messages(self, *_args, **kwargs):
        self.messages.append({**kwargs})
        for key in kwargs.keys():
            assert key in {'Entries'}
        return {
            'Successful': [
                {
                    'MessageId': '123'
                }
            ]
        }

    def delete_message(self, *_args, **kwargs):
        for key in kwargs.keys():
            assert key in {'ReceiptHandle'}
        return {}
