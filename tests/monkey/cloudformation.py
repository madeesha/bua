from botocore.exceptions import ClientError


class MonkeyPatchCloudformationClient:

    def __init__(self):
        self._describe_stacks_count = 0
        self._describe_stacks_responses = []
        self._describe_change_set = 0
        self._describe_change_set_responses = []
        self._create_change_set = 0
        self._create_change_set_responses = []

    def describe_stacks_responses(self, *responses):
        self._describe_stacks_responses.extend(responses)

    def describe_change_set_responses(self, *responses):
        self._describe_change_set_responses.extend(responses)

    def create_change_set_responses(self, *responses):
        self._create_change_set_responses.extend(responses)

    def patch(self):
        self._describe_stacks_count = 0
        self._describe_stacks_responses = []
        self._describe_change_set = 0
        self._describe_change_set_responses = []
        self._create_change_set = 0
        self._create_change_set_responses = []

    def describe_stacks(self, *args, **kwargs):
        self._describe_stacks_count += 1
        return self._describe_stacks_responses.pop(0)

    def describe_change_set(self, *args, **kwargs):
        self._describe_change_set += 1
        response = self._describe_change_set_responses.pop(0)
        if isinstance(response, ClientError):
            raise response
        return response

    def create_change_set(self, *args, **kwargs):
        self._create_change_set += 1
        response = self._create_change_set_responses.pop(0)
        if isinstance(response, ClientError):
            raise response
        return response
