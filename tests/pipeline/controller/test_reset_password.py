from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

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
