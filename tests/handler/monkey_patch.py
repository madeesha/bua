import json
import os
from typing import Dict, List

import boto3
import pymysql


class MonkeyPatch:

    def __init__(self):
        self._client = MonkeyPatchClient()
        self._resource = MonkeyPatchResource()
        self._session = MonkeyPatchSession(self._client)
        self._connection = MonkeyPatchConnection()
        self._environ = dict()
        self._logs = []

    def Session(self):
        return self._session

    def client(self, *args, **kwargs):
        return self._client

    def resource(self, *args, **kwargs):
        return self._resource

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
        self._client.patch()
        self._resource.patch()
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


class MonkeyPatchResource:
    def __init__(self):
        self._table = MonkeyPatchTable()
        self._queue = MonkeyPatchQueue()

    def Table(self, *args, **kwargs):
        return self._table

    def Queue(self, *args, **kwargs):
        return self._queue

    def patch(self):
        self._table.patch()
        self._queue.patch()


class MonkeyPatchClient:

    def put_object(self, *args, **kwargs):
        return {}

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
    def __init__(self, client):
        self._client = client

    def client(self, *args, **kwargs):
        return self._client

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
        self._execute_invocations = 0
        self._result_sets: List[List[Dict]] = []
        self._result_set = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, *args, **kwargs):
        self._result_set = self._result_sets[self._execute_invocations] \
            if self._execute_invocations < len(self._result_sets) else []
        self._execute_invocations += 1
        if self.execute_fails:
            raise RuntimeError('Test execute has an error')
        if self.execute_fails_after_invocations > -1:
            if self.execute_fails_after_invocations < self._execute_invocations:
                raise RuntimeError('Test execute has an error')
        return

    def fetchall(self):
        if self._result_set is None:
            raise RuntimeError('fetchall called before execute')
        return self._result_set

    def patch(self):
        self.execute_fails = False
        self.execute_fails_after_invocations = -1
        self._execute_invocations = 0
        self._result_sets = []
        self._result_set = None

    def add_result_set(self, result_set: List[Dict]):
        self._result_sets.append(result_set)


patch = MonkeyPatch()
