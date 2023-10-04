

class TestCase:

    _environ = {
        'buaTableName': '',
        'exportQueueURL': '',
        'failureQueueURL': '',
        'rdsSecretName': '',
        'debugEnabled': 'Yes',
        'buaBucketName': '',
    }

    def test_invoke_handler(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_export import lambda_handler
        event = {}
        context = {}
        lambda_handler(event, context)
