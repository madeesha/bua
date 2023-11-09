from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

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
