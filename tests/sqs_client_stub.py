class SQSClientStub:

    def __init__(self):
        self.messages = []

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
