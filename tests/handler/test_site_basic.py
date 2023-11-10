import json

import pytest
from pymysql import InternalError


class TestCase:

    _environ = {
        'meterdataTableName': '',
        'buaTableName': '',
        'basicQueueURL': '',
        'failureQueueURL': '',
        'rdsSecretName': '',
        'debugEnabled': 'Yes',
        'meterdataBucketName': '',
        'maxReceiveCount': '100',
    }

    def test_invoke_handler(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_basic import lambda_handler
        event = {}
        context = {}
        lambda_handler(event, context)

    def test_invoke_handler_failure(self):
        with pytest.raises(KeyError) as ex:
            import tests.monkey.patch as monkey_patch
            monkey_patch.patch.patch(environ=self._environ)
            from bua.handler.site_basic import lambda_handler
            event = {
                'Records': [
                    {
                        'eventSource': 'aws:sqs'
                    }
                ]
            }
            context = {}
            lambda_handler(event, context)
        assert str(ex.value) == "'messageId'"

    def test_invoke_handler_reconnect_failure(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        with pytest.raises(InternalError) as ex:
            from bua.handler.site_basic import lambda_handler, handler
            handler.log = monkey_patch.patch.log
            monkey_patch.patch.connect().cursor().execute_fails_after_invocations = 0
            body = {
                'run_type': 'ResetBasicRead',
                'db': {
                    'prefix': 'tst',
                    'update_id': '1',
                    'suffix': 'sql',
                    'domain': 'com',
                    'schema': 'turkey',
                }
            }
            event = {
                'Records': [
                    {
                        'eventSource': 'aws:sqs',
                        'messageId': '123',
                        'eventSourceARN': 'aws:sqs',
                        'body': json.dumps(body)
                    }
                ]
            }
            context = {}
            lambda_handler(event, context)
        assert str(ex.value).startswith('Database connection lost')

    def test_basic_read(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        monkey_patch.patch.connect().cursor().add_result_set([dict()])
        from bua.handler.site_basic import lambda_handler
        event = {
            'run_type': 'BasicRead',
            'account_id': 1,
            'today': '2023-09-01',
            'run_date': '2023-09-25',
            'identifier_type': '',
        }
        context = {}
        lambda_handler(event, context)

    def test_reset_basic_read(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        monkey_patch.patch.connect().cursor().add_result_set([dict()])
        from bua.handler.site_basic import lambda_handler
        event = {
            'run_type': 'ResetBasicRead',
            'account_id': 1,
            'today': '2023-09-01',
            'run_date': '2023-09-25',
            'identifier_type': '',
        }
        context = {}
        lambda_handler(event, context)
