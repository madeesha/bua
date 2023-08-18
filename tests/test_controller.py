import yaml
import os
from pytest import fixture
from pytest import mark
from bua.controller import BUAControllerHandler
from bua.actions.kube import KubeCtl
from tests.cf_client_stub import CFClientStub
from tests.ddb_table_stub import DDBTableStub
from tests.eks_client_stub import EKSClientStub
from tests.kubernetes_stub import KubernetesStub
from tests.my_sql_stub import MySQLStub
from tests.rds_client_stub import RDSClientStub
from tests.route53_client_stub import Route53ClientStub
from tests.s3_client_stub import S3ClientStub
from tests.session_stub import SessionStub
from tests.sm_client_stub import SecretsManagerClientStub
from tests.sqs_client_stub import SQSClientStub
from tests.sts_client_stub import STSClientStub


class TestCase:

    @fixture(autouse=True)
    def before(self):
        if os.path.exists(KubeCtl.KUBE_FILEPATH):
            os.remove(KubeCtl.KUBE_FILEPATH)

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
    def ddb(self):
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
    def config(self, failure_queue_url, prefix):
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
            'failure_queue_url': failure_queue_url
        }

    @fixture(autouse=True)
    def handler(self, r53, sm, s3, ddb, sqs, cf, rds, sts, eks, session, config, mysql, kubes):
        return BUAControllerHandler(
            config=config,
            r53_client=r53, sm_client=sm, s3_client=s3, ddb_table=ddb, sqs_client=sqs, cf_client=cf,
            rds_client=rds, sts_client=sts, eks_client=eks, session=session, mysql=mysql, kubes=kubes
        )

    def test_restore_database(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'restore_database',
            'data': {
                'params_id': 11,
                'snapshot_arn': 'arn:123',
                'instance_type': '8xlarge',
                'mysql_version': '8.0.32',
                'instance_class': 'DBInstanceClassR6i'
            },
            'steps': {
                'restore_database': {
                    'action': 'restore_database',
                    'args': {
                        'update_id': update_id,
                        'suffix': suffix
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_check_restore_database(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'check_restore_database',
            'steps': {
                'check_restore_database': {
                    'action': 'check_restore_database',
                    'args': {
                        'update_id': update_id,
                        'suffix': suffix
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_handler_sqs_check_restore_database(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'check_restore_database',
            'steps': {
                'check_restore_database': {
                    'action': 'check_restore_database',
                    'args': {
                        'update_id': update_id,
                        'suffix': suffix
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
        assert yaml.load(sqs.messages[0]['MessageBody'], Loader=yaml.Loader)['result']['status'] == 'COMPLETE'
        sqs.assert_no_failures()

    def test_destroy_database(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'destroy_database',
            'steps': {
                'destroy_database': {
                    'action': 'destroy_database',
                    'args': {
                        'update_id': update_id,
                        'suffix': suffix
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_check_destroy_database(self, handler, sqs, cf, prefix, update_id, suffix):
        cf.describe_stack_responses = {
            f'{prefix}-{update_id}-{suffix}': {}
        }
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'steps': {
                'step1': {
                    'action': 'check_destroy_database',
                    'args': {
                        'update_id': update_id,
                        'suffix': suffix
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_reset_password(self, handler, sqs, rds_secret_id, update_id, suffix, rds_domain_name, schema_name):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
            },
            'steps': {
                'step1': {
                    'action': 'reset_password',
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_disable_workflow_schedules(self, handler, sqs, rds_secret_id, update_id, suffix, rds_domain_name, schema_name):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
            },
            'steps': {
                'step1': {
                    'action': 'disable_workflow_schedules',
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_disable_workflow_instances(self, handler, sqs, rds_secret_id, update_id, suffix, rds_domain_name, schema_name):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
            },
            'steps': {
                'step1': {
                    'action': 'disable_workflow_instances',
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_execute_sql(self, handler, sqs, rds_secret_id, sql_secret_id, update_id, suffix, rds_domain_name, schema_name):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
                'sqlsecret': sql_secret_id,
            },
            'steps': {
                'step1': {
                    'action': 'execute_sql',
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_execute_sql_with_secret(self, handler, sqs, sql_secret_id, rds_secret_id, update_id, suffix, rds_domain_name, schema_name):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
            },
            'steps': {
                'step1': {
                    'action': 'execute_sql',
                    'args': {
                        'sqlsecret': sql_secret_id
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_stats_sample_pages(self, handler, sqs, rds_secret_id, update_id, suffix, rds_domain_name, schema_name):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
            },
            'steps': {
                'step1': {
                    'action': 'stats_sample_pages',
                    'args': {
                        'tables': [
                            {
                                'name': 'AggRead',
                                'sample_pages': 1600
                            }
                        ]
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_scale_replicas(self, handler, sqs):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
            },
            'steps': {
                'step1': {
                    'action': 'scale_replicas',
                    'args': {
                        'deployment': [ 'workflow' ],
                        'namespace': 'core',
                        'replicas': 2
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_set_rds_dns_entry(self, handler, sqs, hosted_zone_id, dns_record_name, rds_domain_name, suffix, update_id):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'hosted_zone_id': hosted_zone_id,
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
            },
            'steps': {
                'step1': {
                    'action': 'set_rds_dns_entry',
                    'args': {
                        'route53_records': [
                            {
                                'name': dns_record_name,
                                'type': 'CNAME'
                            }
                        ]
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_core_warm_database_statistics(self, handler, sqs, mysql, rds_secret_id, hosted_zone_id, update_id, suffix, rds_domain_name, schema_name):
        mysql.resultsets = [
            [
                {
                    'id': 123
                }
            ]
        ]
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'hosted_zone_id': hosted_zone_id,
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id
            },
            'steps': {
                'step1': {
                    'action': 'core_warm_database_statistics',
                    'args': {
                        'concurrency': 32
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_core_warm_database_indexes(self, handler, sqs, mysql, rds_secret_id, hosted_zone_id, update_id, suffix, rds_domain_name, schema_name):
        mysql.resultsets = [
            [
                {
                    'id': 123
                }
            ]
        ]
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'hosted_zone_id': hosted_zone_id,
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id
            },
            'steps': {
                'step1': {
                    'action': 'core_warm_database_indexes',
                    'args': {
                        'concurrency': 32
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    @mark.parametrize("resultsets", [
        [[{'id': 123}], [{'status': 'NEW', 'total': 100}]],
        [[{'id': 123}], [{'status': 'READY', 'total': 100}]],
        [[{'id': 123}], [{'status': 'INPROG', 'total': 100}]],
        [[{'id': 123}], [{'status': 'DONE', 'total': 100}]],
    ])
    def test_wait_for_workflows(self, handler, sqs, mysql, rds_secret_id, update_id, suffix, rds_domain_name, schema_name, resultsets):
        mysql.resultsets = resultsets
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id
            },
            'steps': {
                'step1': {
                    'action': 'wait_for_workflows',
                    'args': {
                        'workflow_name': 'ExecuteSQL'
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_bua_initiate(self, handler, sqs, mysql, rds_secret_id, update_id, suffix, rds_domain_name, schema_name):
        mysql.resultsets = [[{'id': 123}]]
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id
            },
            'steps': {
                'step1': {
                    'action': 'bua_initiate',
                    'args': {
                        'run_type': 'Utility',
                        'run_date': '2023-08-17',
                        'today': '2023-05-01',
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_wait_for_empty_site_queues_retry(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'Attributes': {
                'ApproximateNumberOfMessages': 100,
                'ApproximateNumberOfMessagesNotVisible': 100,
                'ApproximateNumberOfMessagesDelayed': 100
            }
        }
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
            },
            'steps': {
                'step1': {
                    'action': 'wait_for_empty_site_queues',
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
        sqs.assert_retry_status()

    def test_wait_for_empty_site_queues_complete(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'Attributes': {
                'ApproximateNumberOfMessages': 0,
                'ApproximateNumberOfMessagesNotVisible': 0,
                'ApproximateNumberOfMessagesDelayed': 0
            }
        }
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
            },
            'steps': {
                'step1': {
                    'action': 'wait_for_empty_site_queues',
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
        sqs.assert_complete_status()

    def test_bua_resolve_variances(self, handler, sqs, mysql, rds_secret_id, update_id, suffix, rds_domain_name, schema_name):
        mysql.resultsets = [[{'id': 123}]]
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id
            },
            'steps': {
                'step1': {
                    'action': 'bua_resolve_variances',
                    'args': {
                        'run_date': '2023-08-17',
                        'today': '2023-05-01',
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
