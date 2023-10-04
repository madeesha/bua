class TestCase:

    _environ = {
        'buaTableName': '',
        'projectVersion': '',
        'resourcePrefix': '',
        'className': '',
        'environmentName': '',
        'awsRegion': '',
        'awsAccount': '',
        'nextQueueURL': '',
        'failureQueueURL': '',
        'initiateQueueURL': '',
        'bucketName': '',
    }

    def test_invoke_handler(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.pipeline_controller import lambda_handler
        event = {
            'name': '',
            'this': ''
        }
        context = {}
        lambda_handler(event, context)

    def test_get_config(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.pipeline_controller import lambda_handler
        event = {
            'action': 'get_config'
        }
        context = {}
        lambda_handler(event, context)
