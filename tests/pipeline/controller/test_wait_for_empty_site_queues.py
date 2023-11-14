from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_wait_for_empty_site_queues_retry(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'tst-anstead-sqs-bua-site-basic': {
                'Attributes': {
                    'ApproximateNumberOfMessages': 100,
                    'ApproximateNumberOfMessagesNotVisible': 100,
                    'ApproximateNumberOfMessagesDelayed': 100
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
                    'action': 'wait_for_empty_site_queues',
                }
            }
        }
        response = handler.handle_request(body)
        assert response['result']['status'] == 'RETRY'
        sqs.assert_no_failures()

    def test_wait_for_empty_site_queues_complete(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'tst-anstead-sqs-bua-site-basic': {
                'Attributes': {
                    'ApproximateNumberOfMessages': 0,
                    'ApproximateNumberOfMessagesNotVisible': 0,
                    'ApproximateNumberOfMessagesDelayed': 0
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
                    'action': 'wait_for_empty_site_queues',
                }
            }
        }
        response = handler.handle_request(body)
        assert response['result']['status'] == 'COMPLETE'
        sqs.assert_no_failures()

    def test_wait_for_empty_site_queues_failed(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'tst-anstead-sqs-bua-site-basic-failure-queue': {
                'Attributes': {
                    'ApproximateNumberOfMessages': 1,
                    'ApproximateNumberOfMessagesNotVisible': 0,
                    'ApproximateNumberOfMessagesDelayed': 0
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
                    'action': 'wait_for_empty_site_queues',
                }
            }
        }
        response = handler.handle_request(body)
        assert response['result']['status'] == 'FAILED'
        sqs.assert_no_failures()

    def test_wait_for_empty_site_queues_complete_old_failures(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'tst-anstead-sqs-bua-site-basic-failure-queue': {
                'Attributes': {
                    'ApproximateNumberOfMessages': 1,
                    'ApproximateNumberOfMessagesNotVisible': 0,
                    'ApproximateNumberOfMessagesDelayed': 0
                }
            }
        }
        body = {
            'name': 'Test',
            'this': 'step1',
            'data': {
                'queue_depth': {
                    'tst-anstead-sqs-bua-site-basic-failure-queue': 1
                }
            },
            'steps': {
                'step1': {
                    'action': 'wait_for_empty_site_queues',
                }
            }
        }
        response = handler.handle_request(body)
        assert response['result']['status'] == 'COMPLETE'
        sqs.assert_no_failures()

    def test_wait_for_empty_site_queues_complete_new_failures(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'tst-anstead-sqs-bua-site-basic-failure-queue': {
                'Attributes': {
                    'ApproximateNumberOfMessages': 2,
                    'ApproximateNumberOfMessagesNotVisible': 0,
                    'ApproximateNumberOfMessagesDelayed': 0
                }
            }
        }
        body = {
            'name': 'Test',
            'this': 'step1',
            'data': {
                'queue_depth': {
                    'tst-anstead-sqs-bua-site-basic-failure-queue': 1
                }
            },
            'steps': {
                'step1': {
                    'action': 'wait_for_empty_site_queues',
                }
            }
        }
        response = handler.handle_request(body)
        assert response['result']['status'] == 'FAILED'
        sqs.assert_no_failures()

    def test_wait_for_empty_site_queues_complete_new_dlqueue(self, handler, sqs):
        sqs.get_queue_attributes_response = {
            'tst-anstead-sqs-bua-site-basic-dlqueue': {
                'Attributes': {
                    'ApproximateNumberOfMessages': 2,
                    'ApproximateNumberOfMessagesNotVisible': 0,
                    'ApproximateNumberOfMessagesDelayed': 0
                }
            }
        }
        body = {
            'name': 'Test',
            'this': 'step1',
            'data': {
                'queue_depth': {
                    'tst-anstead-sqs-bua-site-basic-dlqueue': 1
                }
            },
            'steps': {
                'step1': {
                    'action': 'wait_for_empty_site_queues',
                }
            }
        }
        response = handler.handle_request(body)
        assert response['result']['status'] == 'FAILED'
        sqs.assert_no_failures()
