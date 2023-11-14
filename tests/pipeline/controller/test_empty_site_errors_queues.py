from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_empty_site_errors_queues(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'tst-anstead-sqs-bua-site-basic-failure-queue': {
                'Attributes': {
                    'ApproximateNumberOfMessages': 100,
                    'ApproximateNumberOfMessagesNotVisible': 100,
                    'ApproximateNumberOfMessagesDelayed': 100
                }
            },
            'tst-anstead-sqs-bua-site-mscalar-failure-queue': {
                'Attributes': {
                    'ApproximateNumberOfMessages': 10,
                    'ApproximateNumberOfMessagesNotVisible': 10,
                    'ApproximateNumberOfMessagesDelayed': 10
                }
            }
        }
        body = {
            'name': 'Test',
            'this': 'step1',
            'data': {
            },
            'steps': {
                'step1': {
                    'action': 'empty_site_errors_queues',
                }
            }
        }
        response = handler.handle_request(body)
        assert response['result']['status'] == 'COMPLETE'
        assert response['result']['reason'] == 'Purged 330 messages from 2 queues'
        sqs.assert_no_failures()
