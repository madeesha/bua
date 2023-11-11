import yaml


class SQSClientStub:

    def __init__(self, failure_queue_url):
        self.messages = []
        self.failure_queue_url = failure_queue_url
        self.get_queue_attributes_response = {
            'Attributes': {
                'ApproximateNumberOfMessages': 100,
                'ApproximateNumberOfMessagesNotVisible': 100,
                'ApproximateNumberOfMessagesDelayed': 100
            }
        }

    def assert_no_failures(self):
        for message in self.messages:
            assert message['QueueUrl'] != self.failure_queue_url

    def assert_no_messages(self):
        assert len(self.messages) == 0

    def assert_retry_status(self):
        for message in self.messages:
            body = yaml.load(message['MessageBody'], yaml.Loader)
            assert body['result']['status'] == 'RETRY', f'Unexpected {message}'

    def assert_complete_status(self):
        for message in self.messages:
            body = yaml.load(message['MessageBody'], yaml.Loader)
            assert body['result']['status'] == 'COMPLETE', f'Unexpected {message}'

    def send_message(self, *args, **kwargs):
        self.messages.append({**kwargs})
        for key in kwargs.keys():
            assert key in {'QueueUrl', 'MessageBody', 'DelaySeconds'}
        return {
            'MessageId': '123'
        }

    def delete_message(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'QueueUrl', 'ReceiptHandle'}
        return {}

    def list_queues(self, **kwargs):
        for key in kwargs.keys():
            assert key in {'QueueNamePrefix', 'MaxResults'}
        return {
            'QueueUrls': [
                'queue1'
            ]
        }

    def get_queue_attributes(self, **kwargs):
        for key in kwargs.keys():
            assert key in {'QueueUrl', 'AttributeNames'}, f'Unexpected key {key}'
        return self.get_queue_attributes_response


class SQSQueueStub:

    def __init__(self):
        self.messages = []
        self.get_queue_attributes_response = {
            'Attributes': {
                'ApproximateNumberOfMessages': 100,
                'ApproximateNumberOfMessagesNotVisible': 100,
                'ApproximateNumberOfMessagesDelayed': 100
            }
        }

    def assert_no_messages(self):
        assert self.messages == []

    def send_message(self, *args, **kwargs):
        self.messages.append({**kwargs})
        for key in kwargs.keys():
            assert key in {'MessageBody', 'DelaySeconds'}
        return {
            'MessageId': '123'
        }

    def send_messages(self, *args, **kwargs):
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

    def delete_message(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'ReceiptHandle'}
        return {}
