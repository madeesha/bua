from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

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
