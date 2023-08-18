import yaml
import os
import pytest
from bua.controller import BUAControllerHandler
from bua.actions.kube import KubeCtl


class TestCase:

    @pytest.fixture(autouse=True)
    def before(self):
        if os.path.exists(KubeCtl.KUBE_FILEPATH):
            os.remove(KubeCtl.KUBE_FILEPATH)

    def test_handler_invoke_lambda_check_restore_database(self):
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
        r53 = Route53Stub()
        sm = SecretsManagerStub()
        s3 = S3Stub()
        ddb = TableStub()
        sqs = SQSStub()
        cf = CFStub()
        rds = RDSStub()
        sts = STSStub()
        eks = EKSStub()
        session = SessionStub()
        config = {
            'version': '123',
            'prefix': 'tst-anstead',
            'cluster': 'anstead',
            'env': 'tst',
            'region': 'ap-southeast-2',
            'secret': {
                'username': 'core_admin',
                'password': '123'
            },
            'next_queue_url': '123',
            'failure_queue_url': '123'
        }
        handler = BUAControllerHandler(config=config,
                                       r53_client=r53, sm_client=sm, s3_client=s3, ddb_table=ddb, sqs_client=sqs, cf_client=cf, rds_client=rds, sts_client=sts, eks_client=eks, session=session)
        handler.handle_request(body)

    def test_handler_sqs_check_restore_database(self):
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
        r53 = Route53Stub()
        sm = SecretsManagerStub()
        s3 = S3Stub()
        ddb = TableStub()
        sqs = SQSStub()
        cf = CFStub()
        rds = RDSStub()
        sts = STSStub()
        eks = EKSStub()
        session = SessionStub()
        config = {
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
        handler = BUAControllerHandler(config=config,
                                       r53_client=r53, sm_client=sm, s3_client=s3, ddb_table=ddb, sqs_client=sqs, cf_client=cf, rds_client=rds, sts_client=sts, eks_client=eks, session=session)
        handler.handle_request(event)
        assert len(sqs.messages) == 1
        assert sqs.messages[0]['QueueUrl'] == 'next_queue'
        assert yaml.load(sqs.messages[0]['MessageBody'], Loader=yaml.Loader)['result']['status'] == 'RETRY'


class CFStub:
    def describe_stacks(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'StackName'}
        return {
            'Stacks': [
                {

                }
            ]
        }


class RDSStub:
    def modify_db_instance(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'DBInstanceIdentifier', 'MasterUserPassword'}
        return {}


class STSStub:
    pass


class EKSStub:
    def describe_cluster(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'name'}
        return {
            'cluster': {
                'certificateAuthority': {
                    'data': '123'
                },
                'endpoint': '123'
            }
        }


class SessionStub:
    def __init__(self):
        self.region_name = 'ap-southeast-2'


class SQSStub:

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


class TableStub:

    def put_item(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Item', 'ConditionExpression'}
        return {}

    def get_item(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Key', 'ConsistentRead'}
        return {}


class S3Stub:

    def put_object(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Bucket', 'Key', 'ContentMD5', 'ContentType', 'ContentLength', 'Body'}
        return {}

    def get_object(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Bucket', 'Key'}
        return {}


class SecretsManagerStub:
    pass


class Route53Stub:
    pass
