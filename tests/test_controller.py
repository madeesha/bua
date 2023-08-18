import yaml
import os
import pytest
from bua.controller import BUAControllerHandler
from bua.actions.kube import KubeCtl
from tests.cf_client_stub import CFClientStub
from tests.ddb_table_stub import DDBTableStub
from tests.eks_client_stub import EKSClientStub
from tests.rds_client_stub import RDSClientStub
from tests.route53_client_stub import Route53ClientStub
from tests.s3_client_stub import S3ClientStub
from tests.session_stub import SessionStub
from tests.sm_client_stub import SecretsManagerClientStub
from tests.sqs_client_stub import SQSClientStub
from tests.sts_client_stub import STSClientStub


class TestCase:

    @pytest.fixture(autouse=True)
    def before(self):
        if os.path.exists(KubeCtl.KUBE_FILEPATH):
            os.remove(KubeCtl.KUBE_FILEPATH)

    @pytest.fixture(autouse=True)
    def r53(self):
        return Route53ClientStub()

    @pytest.fixture(autouse=True)
    def sm(self):
        return SecretsManagerClientStub()

    @pytest.fixture(autouse=True)
    def s3(self):
        return S3ClientStub()

    @pytest.fixture(autouse=True)
    def ddb(self):
        return DDBTableStub()

    @pytest.fixture(autouse=True)
    def sqs(self):
        return SQSClientStub()

    @pytest.fixture(autouse=True)
    def cf(self):
        return CFClientStub()

    @pytest.fixture(autouse=True)
    def rds(self):
        return RDSClientStub()

    @pytest.fixture(autouse=True)
    def sts(self):
        return STSClientStub()

    @pytest.fixture(autouse=True)
    def eks(self):
        return EKSClientStub()

    @pytest.fixture(autouse=True)
    def session(self):
        return SessionStub()

    @pytest.fixture(autouse=True)
    def config(self):
        return {
            'version': '123',
            'prefix': 'tst-anstead',
            'cluster': 'anstead',
            'env': 'tst',
            'region': 'ap-southeast-2',
            'secret': {
                'username': 'core_admin',
                'password': '123'
            },
            'next_queue_url': 'next_queue',
            'failure_queue_url': 'failure_queue'
        }

    @pytest.fixture(autouse=True)
    def handler(self, r53, sm, s3, ddb, sqs, cf, rds, sts, eks, session, config):
        return BUAControllerHandler(
            config=config,
            r53_client=r53, sm_client=sm, s3_client=s3, ddb_table=ddb, sqs_client=sqs, cf_client=cf,
            rds_client=rds, sts_client=sts, eks_client=eks, session=session
        )

    def test_restore_database(self, handler):
        body = {
            'name': 'Restore Database',
            'this': 'restore_database',
            'steps': {
                'restore_database': {
                    'action': 'restore_database',
                    'args': {
                        'update_id': '11',
                        'suffix': '20230712-sql'
                    }
                }
            }
        }
        handler.handle_request(body)

    def test_check_restore_database(self, handler):
        body = {
            'name': 'Restore Database',
            'this': 'check_restore_database',
            'steps': {
                'check_restore_database': {
                    'action': 'check_restore_database',
                    'args': {
                        'update_id': '11',
                        'suffix': '20230712-sql'
                    }
                }
            }
        }
        handler.handle_request(body)

    def test_handler_sqs_check_restore_database(self, handler, sqs):
        body = {
            'name': 'Restore Database',
            'this': 'check_restore_database',
            'steps': {
                'check_restore_database': {
                    'action': 'check_restore_database',
                    'args': {
                        'update_id': '11',
                        'suffix': '20230712-sql'
                    },
                    'on': {
                        'COMPLETE': {
                            'next': 'next_step'
                        }
                    }
                }
            }
        }
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'eventSourceARN': '123',
                    'messageId': '123',
                    'receiptHandle': '123',
                    'body': yaml.dump(body)
                }
            ]
        }
        handler.handle_request(event)
        assert len(sqs.messages) == 1
        assert sqs.messages[0]['QueueUrl'] == 'next_queue'
        assert yaml.load(sqs.messages[0]['MessageBody'], Loader=yaml.Loader)['result']['status'] == 'RETRY'

    def test_destroy_database(self, handler):
        body = {
            'name': 'Restore Database',
            'this': 'destroy_database',
            'steps': {
                'destroy_database': {
                    'action': 'destroy_database',
                    'args': {
                        'update_id': '11',
                        'suffix': '20230712-sql'
                    }
                }
            }
        }
        handler.handle_request(body)
