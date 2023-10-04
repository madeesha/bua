class TestCase:

    _environ = {
        'meterdataTableName': '',
        'buaTableName': '',
        'mscalarQueueURL': '',
        'failureQueueURL': '',
        'rdsSecretName': '',
        'debugEnabled': 'Yes',
        'meterdataBucketName': '',
    }

    def test_invoke_handler(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.site_mscalar import lambda_handler
        event = {}
        context = {}
        lambda_handler(event, context)
