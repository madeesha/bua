class MonkeyPatchSQSResource:
    def __init__(self):
        self._queue = MonkeyPatchQueue()

    def Queue(self, *args, **kwargs):
        return self._queue

    def patch(self):
        self._queue.patch()


class MonkeyPatchSQSClient:

    def __init__(self):
        self._send_message = []

    def patch(self):
        self._send_message = []

    def send_message(self, *args, **kwargs):
        self._send_message.append((args, kwargs))
        return {
            'MessageId': ''
        }

    def assert_no_messages(self):
        assert len(self._send_message) == 0, self._send_message


class MonkeyPatchQueue:
    def patch(self):
        pass

    def send_message(self, MessageBody):
        return {
            'MessageId': '123'
        }
