from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_ili_manual_line_item_exceptions(
            self, handler, sqs, update_id, suffix, rds_domain_name, schema_name, rds_secret_id, print
    ):
        body = {
            'name': 'ILI Exceptions',
            'this': 'ili_manual_line_item_exceptions',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
                'end_inclusive': '2023-01-01',
            },
            'steps': {
                'ili_manual_line_item_exceptions': {
                    'action': 'ili_manual_line_item_exceptions'
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
        assert len(print.lines) == 4
        print.assert_has_line("CALL ili_manual_line_item_exceptions(-1, 1, -1, '2023-01-01')")
