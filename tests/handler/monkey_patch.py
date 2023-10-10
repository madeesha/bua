import json
import os
from typing import Dict, List

import boto3
import pymysql
from botocore.exceptions import ClientError


class MonkeyPatch:

    def __init__(self):
        self._clients = {
            's3': MonkeyPatchS3Client(),
            'secretsmanager': MonkeyPatchSecretsManagerClient(),
            'sqs': MonkeyPatchSQSClient(),
            'cloudformation': MonkeyPatchCloudformationClient(),
            'rds': MonkeyPatchRDSClient(),
            'sts': MonkeyPatchSTSClient(),
            'eks': MonkeyPatchEKSClient(),
            'route53': MonkeyPatchRoute53Client(),
        }
        self._resources = {
            'dynamodb': MonkeyPatchDynamoDBResource(),
            'sqs': MonkeyPatchSQSResource(),
        }
        self._session = MonkeyPatchSession(self._clients, self._resources)
        self._connection = MonkeyPatchConnection()
        self._environ = dict()
        self._logs = []

    def cloudformation(self):
        return self._clients['cloudformation']

    def sqs(self):
        return self._clients['sqs']

    def Session(self):
        return self._session

    def client(self, name, *args, **kwargs):
        return self._clients[name]

    def resource(self, name, *args, **kwargs):
        return self._resources[name]

    def connect(self, *args, **kwargs):
        return self._connection

    @property
    def region_name(self):
        return ''

    @property
    def environ(self):
        return self._environ

    def patch(self, *, environ: Dict):
        self._environ = environ
        for client in self._clients.values():
            client.patch()
        for resource in self._resources.values():
            resource.patch()
        self._session.patch()
        self._connection.patch()
        os.environ = self.environ
        boto3.session = self
        boto3.Session = self.Session
        boto3.client = self.client
        boto3.resource = self.resource
        pymysql.connect = self.connect

    def log(self, *args, **kwargs):
        print(*args, **kwargs)
        self._logs.append((args, kwargs))

    def assert_log(self, msg):
        for _args, _kwargs in self._logs:
            if msg in _args:
                return
        assert False, f'{msg} not in {self._logs}'


class MonkeyPatchDynamoDBResource:
    def __init__(self):
        self._table = MonkeyPatchTable()

    def Table(self, *args, **kwargs):
        return self._table

    def patch(self):
        self._table.patch()


class MonkeyPatchSQSResource:
    def __init__(self):
        self._queue = MonkeyPatchQueue()

    def Queue(self, *args, **kwargs):
        return self._queue

    def patch(self):
        self._queue.patch()


class MonkeyPatchS3Client:

    def put_object(self, *args, **kwargs):
        return {}

    def patch(self):
        pass


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


class MonkeyPatchRDSClient:

    def patch(self):
        pass


class MonkeyPatchSTSClient:

    def patch(self):
        pass


class MonkeyPatchEKSClient:

    def patch(self):
        pass


class MonkeyPatchRoute53Client:

    def patch(self):
        pass


class MonkeyPatchSecretsManagerClient:

    def get_secret_value(self, *args, **kwargs):
        return {
            'SecretString': json.dumps({
                'rdshost': '',
                'username': '',
                'password': '',
                'dbname': ''
            })
        }

    def patch(self):
        pass


class MonkeyPatchSession:
    def __init__(self, clients, resources):
        self._clients = clients
        self._resources = resources

    def client(self, name, *args, **kwargs):
        return self._clients[name]

    def resource(self, name, *args, **kwargs):
        return self._resources[name]

    @property
    def region_name(self):
        return ''

    def patch(self):
        pass


class MonkeyPatchTable:

    def get_item(self, *args, **kwargs):
        return {}

    def put_item(self, *args, **kwargs):
        return {}

    def patch(self):
        pass


class MonkeyPatchQueue:
    def patch(self):
        pass


class MonkeyPatchConnection:
    def __init__(self):
        self._cursor = MonkeyPatchCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        return

    def rollback(self):
        return

    def patch(self):
        self._cursor.patch()


class MonkeyPatchCursor:

    def __init__(self):
        self.execute_fails = False
        self.execute_fails_after_invocations = -1
        self._execute_invocations = []
        self._result_sets: List[List[Dict]] = []
        self._result_set = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def patch(self):
        self.execute_fails = False
        self.execute_fails_after_invocations = -1
        self._execute_invocations = []
        self._result_sets = []
        self._result_set = None

    def assert_n_execute_invocations(self, n=0):
        assert len(self._execute_invocations) == n, self._execute_invocations

    def execute(self, *args, **kwargs):
        self._result_set = self._result_sets[len(self._execute_invocations)] \
            if len(self._execute_invocations) < len(self._result_sets) else []
        self._execute_invocations.append((args, kwargs))
        if self.execute_fails:
            raise RuntimeError('Test execute has an error')
        if self.execute_fails_after_invocations > -1:
            if self.execute_fails_after_invocations < len(self._execute_invocations):
                raise RuntimeError('Test execute has an error')
        return

    def fetchall(self):
        if self._result_set is None:
            raise RuntimeError('fetchall called before execute')
        return self._result_set

    def add_result_set(self, result_set: List[Dict]):
        self._result_sets.append(result_set)


patch = MonkeyPatch()
