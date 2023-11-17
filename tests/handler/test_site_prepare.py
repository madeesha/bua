import json

import pytest

from tests.monkey.logs import Logs


class TestCase:

    _environ = {
        'buaTableName': 'bua-table',
        'prepareQueueURL': 'prepare-queue',
        'failureQueueURL': 'failure-queue',
        'rdsSecretName': 'rds-secret',
        'debugEnabled': 'Yes',
        'buaBucketName': 'bua-bucket',
        'maxReceiveCount': '100',
    }

    def test_invoke_handler_single_message(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_prepare import lambda_handler
        from bua.handler.site_prepare import handler
        logs = Logs(handler)
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
        lambda_handler(event, context)
        assert len(logs.find_logs_with_args({'status': 'DONE'})) == 1
        assert len(monkey_patch.patch.mysqs.get_queue('failure-queue').messages) == 0

    def test_invoke_handler_multiple_messages(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_prepare import lambda_handler
        from bua.handler.site_prepare import handler
        logs = Logs(handler)
        body1 = {
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
        body2 = {
            'run_type': 'PrepareExport',
            'start_inclusive': '2022-10-01',
            'end_inclusive': '2023-09-30',
            'end_exclusive': '2023-10-01',
            'today': '2023-10-01',
            'run_date': '2023-10-01',
            'identifier_type': 'Segment1',
            'account_id': '456',
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
                    'body': json.dumps(body1),
                },
                {
                    'eventSource': 'aws:sqs',
                    'messageId': '456',
                    'eventSourceARN': 'aws:sqs',
                    'body': json.dumps(body2),
                }
            ]
        }
        context = {}
        lambda_handler(event, context)
        assert len(logs.find_logs_with_args({'status': 'DONE'})) == 2
        assert len(monkey_patch.patch.mysqs.get_queue('failure-queue').messages) == 0

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
