import yaml

from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_check_restore_database(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'check_restore_database',
            'steps': {
                'check_restore_database': {
                    'action': 'check_restore_database',
                    'args': {
                        'update_id': update_id,
                        'suffix': suffix
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

    def test_handler_sqs_check_restore_database(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'check_restore_database',
            'steps': {
                'check_restore_database': {
                    'action': 'check_restore_database',
                    'args': {
                        'update_id': update_id,
                        'suffix': suffix
                    },
                    'on': {
                        'COMPLETE': {
                            'next': 'next_step'
                        }
                    }
                },
                'next_step': {
                    'comment': 'Do something'
                }
            }
        }
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'eventSourceARN': '123',
                    'messageId': '123',
                    'receiptHandle': '123',
                    'body': yaml.dump(body)
                }
            ]
        }
        handler.handle_request(event)
        assert len(sqs.messages) == 1
        assert sqs.messages[0]['QueueUrl'] == 'next_queue'
        assert yaml.load(sqs.messages[0]['MessageBody'], Loader=yaml.Loader)['result']['status'] == 'COMPLETE'
        sqs.assert_no_failures()

