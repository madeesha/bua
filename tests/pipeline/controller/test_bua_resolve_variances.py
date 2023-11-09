from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

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
