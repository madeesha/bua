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
