from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_record_site_errors_queues(self, handler, sqs):
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
                    'action': 'record_site_errors_queues',
                }
            }
        }
        response = handler.handle_request(body)
        assert response['result']['status'] == 'COMPLETE'
        assert response['result']['reason'] == 'Recorded 2 error queue depths'
        assert response['data']['queue_depth'] == {
            'tst-anstead-sqs-bua-site-basic-failure-queue': 300,
            'tst-anstead-sqs-bua-site-mscalar-failure-queue': 30,
        }
        sqs.assert_no_failures()
