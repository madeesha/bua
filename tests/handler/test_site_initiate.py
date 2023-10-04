import pytest


class TestCase:

    _environ = {
        'meterdataTableName': '',
        'buaTableName': '',
        'dataQueueURL': '',
        'segmentQueueURL': '',
        'exportQueueURL': '',
        'failureQueueURL': '',
        'basicQueueURL': '',
        'mscalarQueueURL': '',
        'nem12QueueURL': '',
        'rdsSecretName': '',
        'debugEnabled': 'Yes',
        'utilityBatchSize': '10',
        'jurisdictionBatchSize': '10',
        'tniBatchSize': '10',
    }

    def test_invoke_handler(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_initiate import lambda_handler
        event = {}
        context = {}
        lambda_handler(event, context)

    def test_invoke_handler_failure(self):
        with pytest.raises(RuntimeError):
            import tests.handler.monkey_patch as monkey_patch
            monkey_patch.patch.patch(environ=self._environ)
            from bua.handler.site_initiate import lambda_handler
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
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        with pytest.raises(RuntimeError) as ex:
            from bua.handler.site_initiate import lambda_handler, handler
            handler.log = monkey_patch.patch.log
            monkey_patch.patch.connect().cursor().execute_fails_after_invocations = 0
            event = {
                'Records': [
                    {
                        'eventSource': 'aws:sqs'
                    }
                ]
            }
            context = {}
            lambda_handler(event, context)
        assert str(ex.value).startswith('Failed to handle request')
        monkey_patch.patch.assert_log('Failed to reconnect to the database after a failure')
