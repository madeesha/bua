from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

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
