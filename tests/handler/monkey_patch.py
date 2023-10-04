import json
import os
from typing import Dict

import boto3
import pymysql


class MonkeyPatch:

    def __init__(self):
        self._client = MonkeyPatchClient()
        self._resource = MonkeyPatchResource()
        self._session = MonkeyPatchSession(self._client)
        self._connection = MonkeyPatchConnection()
        self._environ = dict()

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
        return {
            'buaTableName': '',
            'projectVersion': '',
            'resourcePrefix': '',
            'className': '',
            'environmentName': '',
            'awsRegion': '',
            'awsAccount': '',
            'nextQueueURL': '',
            'failureQueueURL': '',
            'initiateQueueURL': '',
            'bucketName': '',
            'tableName': '',
            'meterdataTableName': '',
            'basicQueueURL': '',
            'rdsSecretName': '',
            'debugEnabled': '',
            'meterdataBucketName': '',
            'siteDataQueueURL': '',
            'checkNEM': '',
            'checkAggRead': '',
            'exportQueueURL': '',
            'buaBucketName': '',
            'dataQueueURL': '',
            'segmentQueueURL': '',
            'mscalarQueueURL': '',
            'nem12QueueURL': '',
            'utilityBatchSize': '10',
            'jurisdictionBatchSize': '10',
            'tniBatchSize': '10'
        }

    def patch(self, *, environ: Dict):
        self._environ = environ
        os.environ = self.environ
        boto3.session = self
        boto3.Session = self.Session
        boto3.client = self.client
        boto3.resource = self.resource
        pymysql.connect = self.connect


class MonkeyPatchResource:
    def __init__(self):
        self._table = MonkeyPatchTable()
        self._queue = MonkeyPatchQueue()

    def Table(self, *args, **kwargs):
        return self._table

    def Queue(self, *args, **kwargs):
        return self._queue


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


class MonkeyPatchSession:
    def __init__(self, client):
        self._client = client

    def client(self, *args, **kwargs):
        return self._client

    @property
    def region_name(self):
        return ''


class MonkeyPatchTable:

    def get_item(self, *args, **kwargs):
        return {}

    def put_item(self, *args, **kwargs):
        return {}


class MonkeyPatchQueue:
    pass


class MonkeyPatchConnection:
    def __init__(self):
        self._cursor = MonkeyPatchCursor()

    def cursor(self):
        return self._cursor


class MonkeyPatchCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, *args, **kwargs):
        return


patch = MonkeyPatch()
