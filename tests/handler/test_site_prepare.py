import json

import pytest


class TestCase:

    _environ = {
        'buaTableName': '',
        'prepareQueueURL': '',
        'failureQueueURL': '',
        'rdsSecretName': '',
        'debugEnabled': 'Yes',
        'buaBucketName': '',
        'maxReceiveCount': '100',
    }

    def test_invoke_handler(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_prepare import lambda_handler
        body = {
            'run_type': 'PrepareExport',
            'start_inclusive': '2022-10-01',
            'end_inclusive': '2023-09-30',
            'end_exclusive': '2023-10-01',
            'today': '2023-10-01',
            'run_date': '2023-10-01',
            'identifier_type': 'Segment1',
            'account_id': '123',
            'db': {
                'prefix': 'tst',
                'update_id': '1',
                'suffix': 'sql',
                'domain': 'com',
                'schema': 'turkey'
            }
        }
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'messageId': '123',
                    'eventSourceARN': 'aws:sqs',
                    'body': json.dumps(body),
                }
            ]
        }
        context = {}
        response = lambda_handler(event, context)
        assert response == {
            'status': 'DONE'
        }
        monkey_patch.patch.sqs().assert_no_messages()

    def test_invoke_handler_failure(self):
        with pytest.raises(KeyError):
            import tests.monkey.patch as monkey_patch
            monkey_patch.patch.patch(environ=self._environ)
            from bua.handler.site_prepare import lambda_handler
            event = {
                'Records': [
                    {
                        'eventSource': 'aws:sqs'
                    }
                ]
            }
            context = {}
            lambda_handler(event, context)
