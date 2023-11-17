import json

from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_bua_initiate(
            self, handler, sqs, mysql, rds_secret_id, update_id, suffix, rds_domain_name, schema_name, prefix
    ):
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
                        'source_date': '2023-08-17',
                        'today': '2023-05-01',
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
        assert json.loads(sqs.messages[0]['MessageBody']) == {
            'run_type': 'Utility',
            'run_date': '2023-08-17',
            'today': '2023-05-01',
            'start_inclusive': '2022-11-01',
            'end_exclusive': '2023-11-01',
            'end_inclusive': '2023-10-31',
            'source_date': '2023-08-17',
            'current_date': body['data']['current_date'],
            'current_time': body['data']['current_time'],
            'identifier_type': None,
            'db': {
                'prefix': prefix,
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
            }
        }
