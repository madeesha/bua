from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_bua_prepare_billing_threshold(
            self, handler, sqs, update_id, suffix, rds_domain_name, schema_name, rds_secret_id
    ):
        body = {
            'name': 'Prepare Billing Threshold',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
                'end_inclusive': '2023-10-31'
            },
            'steps': {
                'step1': {
                    'action': 'bua_prepare_billing_threshold'
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
