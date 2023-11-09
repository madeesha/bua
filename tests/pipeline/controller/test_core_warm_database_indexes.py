from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

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
