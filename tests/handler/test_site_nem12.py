import json

import pytest
from pymysql import InternalError


class TestCase:

    _environ = {
        'meterdataTableName': '',
        'buaTableName': '',
        'nem12QueueURL': '',
        'failureQueueURL': '',
        'rdsSecretName': '',
        'debugEnabled': 'Yes',
        'meterdataBucketName': '',
        'maxReceiveCount': '100',
    }

    def test_invoke_handler(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_nem12 import lambda_handler
        event = {}
        context = {}
        lambda_handler(event, context)

    def test_invoke_handler_failure(self):
        with pytest.raises(KeyError):
            import tests.monkey.patch as monkey_patch
            monkey_patch.patch.patch(environ=self._environ)
            from bua.handler.site_nem12 import lambda_handler
            event = {
                'Records': [
                    {
                        'eventSource': 'aws:sqs'
                    }
                ]
            }
            context = {}
            lambda_handler(event, context)

    def test_invoke_handler_reconnect_failure(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        with pytest.raises(InternalError) as ex:
            from bua.handler.site_nem12 import lambda_handler, handler
            handler.log = monkey_patch.patch.log
            monkey_patch.patch.connect().cursor().execute_fails_after_invocations = 0
            event = {
                'Records': [
                    {
                        'eventSource': 'aws:sqs',
                        'messageId': '123',
                        'eventSourceARN': 'aws:123',
                        'body': json.dumps({
                            'run_type': 'NEM12',
                            'nmi': '123',
                            'start_inclusive': '2023-01-01',
                            'end_exclusive': '2023-01-01',
                            'today': '2023-01-01',
                            'run_date': '2023-01-01',
                            'identifier_type': 'ABC',
                            'db': {
                                'prefix': 'tst',
                                'update_id': '1',
                                'suffix': 'sql',
                                'domain': 'com',
                                'schema': 'turkey'
                            }
                        }),
                    }
                ]
            }
            context = {}
            lambda_handler(event, context)
        assert str(ex.value).startswith('Database connection lost')

    def test_invoke_handler_nem12(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_nem12 import lambda_handler
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'messageId': '123',
                    'eventSourceARN': 'queue:1',
                    'body': json.dumps({
                        'run_type': 'NEM12',
                        'nmi': '123',
                        'start_inclusive': None,
                        'end_exclusive': None,
                        'today': '2023-10-01',
                        'run_date': '2023-10-10',
                        'identifier_type': 'SegmentJurisdictionAvgExclEst',
                    })
                }
            ]
        }
        context = {}
        lambda_handler(event, context)
