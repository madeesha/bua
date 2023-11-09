from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

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

