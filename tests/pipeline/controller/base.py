import os

from pytest import fixture

from bua.pipeline.actions.kube import KubeCtl
from bua.pipeline.handler.controller import BUAControllerHandler
from tests.pipeline.stubs.cf_client_stub import CFClientStub
from tests.pipeline.stubs.ddb_table_stub import DDBTableStub
from tests.pipeline.stubs.eks_client_stub import EKSClientStub
from tests.pipeline.stubs.kubernetes_stub import KubernetesStub
from tests.pipeline.stubs.my_sql_stub import MySQLStub
from tests.pipeline.stubs.rds_client_stub import RDSClientStub
from tests.pipeline.stubs.route53_client_stub import Route53ClientStub
from tests.pipeline.stubs.s3_client_stub import S3ClientStub
from tests.pipeline.stubs.session_stub import SessionStub
from tests.pipeline.stubs.sm_client_stub import SecretsManagerClientStub
from tests.pipeline.stubs.sqs_client_stub import SQSClientStub
from tests.pipeline.stubs.ssm_client_stub import SSMClientStub
from tests.pipeline.stubs.sts_client_stub import STSClientStub


class TestBase:

    @fixture(autouse=True)
    def before(self):
        if os.path.exists(KubeCtl.KUBE_FILEPATH):
            os.remove(KubeCtl.KUBE_FILEPATH)

    @fixture(autouse=True)
    def initiate_queue_url(self):
        return 'initiate-queue-url'

    @fixture(autouse=True)
    def failure_queue_url(self):
        return 'failure-queue-url'

    @fixture(autouse=True)
    def sql_secret_id(self):
        return 'sql-secret-key'

    @fixture(autouse=True)
    def rds_secret_id(self):
        return 'rds-secret-key'

    @fixture(autouse=True)
    def hosted_zone_id(self):
        return 'ZONE123'

    @fixture(autouse=True)
    def dns_record_name(self):
        return 'mysql.anstead.encore,'

    @fixture(autouse=True)
    def rds_domain_name(self):
        return 'rds.domain.name'

    @fixture(autouse=True)
    def rds_host_name(self, prefix, update_id, suffix, rds_domain_name):
        return f'{prefix}-{update_id}-{suffix}.{rds_domain_name}'

    @fixture(autouse=True)
    def prefix(self):
        return 'tst-anstead'

    @fixture(autouse=True)
    def suffix(self):
        return 'bua-sql'

    @fixture(autouse=True)
    def update_id(self):
        return 11

    @fixture(autouse=True)
    def schema_name(self):
        return 'TurkeyBLU'

    @fixture(autouse=True)
    def r53(self, hosted_zone_id, dns_record_name, rds_host_name):
        return Route53ClientStub(
            hosted_zone_id=hosted_zone_id,
            start_record_name=dns_record_name,
            start_record_type='CNAME',
            resource_records=[
                {
                    'Value': rds_host_name
                }
            ]
        )

    @fixture(autouse=True)
    def sm(self, sql_secret_id, rds_secret_id):
        secrets = {}
        secrets[sql_secret_id] = {
            'sql': 'sql-statement'
        }
        secrets[rds_secret_id] = {
            'username': 'user1',
            'password': 'pass1',
        }
        return SecretsManagerClientStub(secrets)

    @fixture(autouse=True)
    def s3(self):
        return S3ClientStub()

    @fixture(autouse=True)
    def ddb_table(self):
        return DDBTableStub()

    @fixture(autouse=True)
    def sqs(self, failure_queue_url):
        return SQSClientStub(failure_queue_url)

    @fixture(autouse=True)
    def cf(self, prefix, update_id, suffix):
        describe_stack_responses = {
            f'{prefix}-{update_id}-{suffix}': {
                'Stacks': [
                    {
                        'StackStatus': 'CREATE_COMPLETE'
                    }
                ]
            }
        }
        return CFClientStub(describe_stack_responses=describe_stack_responses)

    @fixture(autouse=True)
    def rds(self):
        return RDSClientStub()

    @fixture(autouse=True)
    def sts(self):
        return STSClientStub()

    @fixture(autouse=True)
    def eks(self):
        return EKSClientStub()

    @fixture(autouse=True)
    def ssm(self):
        parameters = {}
        return SSMClientStub(parameters=parameters)

    @fixture(autouse=True)
    def session(self, sts):
        clients = {
            'sts': sts
        }
        return SessionStub(clients=clients)

    @fixture(autouse=True)
    def mysql(self):
        return MySQLStub([])

    @fixture(autouse=True)
    def kubes(self):
        return KubernetesStub(replicas=1)

    @fixture(autouse=True)
    def config(self, failure_queue_url, initiate_queue_url, prefix):
        return {
            'version': '123',
            'prefix': prefix,
            'cluster': 'anstead',
            'env': 'tst',
            'region': 'ap-southeast-2',
            'secret': {
                'username': 'core_admin',
                'password': '123'
            },
            'next_queue_url': 'next_queue',
            'failure_queue_url': failure_queue_url,
            'initiate_queue_url': initiate_queue_url,
            'mysql80_option_group_name': '',
            'core_kms_key_id': '',
            'aws_account': '123',
        }

    @fixture(autouse=True)
    def handler(self, r53, sm, s3, ddb_table, sqs, cf, rds, sts, eks, ssm, session, config, mysql, kubes):
        return BUAControllerHandler(
            config=config,
            r53_client=r53, sm_client=sm, s3_client=s3, ddb_bua_table=ddb_table, sqs_client=sqs, cf_client=cf,
            rds_client=rds, sts_client=sts, eks_client=eks, ssm_client=ssm, session=session, mysql=mysql, kubes=kubes
        )
