import os
from typing import Dict

import boto3
import pymysql

from tests.monkey.cloudformation import MonkeyPatchCloudformationClient
from tests.monkey.dynamodb import MonkeyPatchDynamoDBResource
from tests.monkey.eks import MonkeyPatchEKSClient
from tests.monkey.mysql import MonkeyPatchConnection
from tests.monkey.rds import MonkeyPatchRDSClient
from tests.monkey.route53 import MonkeyPatchRoute53Client
from tests.monkey.s3 import MonkeyPatchS3Client, MyS3
from tests.monkey.secretsmanager import MonkeyPatchSecretsManagerClient
from tests.monkey.session import MonkeyPatchSession
from tests.monkey.sqs import MonkeyPatchSQSResource, MonkeyPatchSQSClient, MySQS
from tests.monkey.ssm import MonkeyPatchSSMClient
from tests.monkey.stepfunctions import MonkeyPatchStepFunctionsClient
from tests.monkey.sts import MonkeyPatchSTSClient


class MonkeyPatch:

    def __init__(self):
        self.mysqs = MySQS()
        self.mys3 = MyS3()
        self._clients = {
            's3': MonkeyPatchS3Client(mys3=self.mys3),
            'secretsmanager': MonkeyPatchSecretsManagerClient(),
            'sqs': MonkeyPatchSQSClient(mysqs=self.mysqs),
            'cloudformation': MonkeyPatchCloudformationClient(),
            'rds': MonkeyPatchRDSClient(),
            'sts': MonkeyPatchSTSClient(),
            'eks': MonkeyPatchEKSClient(),
            'route53': MonkeyPatchRoute53Client(),
            'stepfunctions': MonkeyPatchStepFunctionsClient(),
            'ssm': MonkeyPatchSSMClient(),
        }
        self._resources = {
            'dynamodb': MonkeyPatchDynamoDBResource(),
            'sqs': MonkeyPatchSQSResource(mysqs=self.mysqs),
        }
        self._session = MonkeyPatchSession(self._clients, self._resources)
        self._connection = MonkeyPatchConnection()
        self._environ = dict()
        self._logs = []

    def cloudformation(self):
        return self._clients['cloudformation']

    def sqs(self):
        return self._clients['sqs']

    def stepfunctions(self):
        return self._clients['stepfunctions']

    def ssm(self):
        return self._clients['ssm']

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


patch = MonkeyPatch()
