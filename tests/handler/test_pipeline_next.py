class TestCase:

    _environ = {
        'tableName': '',
        'bucketName': '',
    }

    def test_invoke_handler(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.pipeline_next import lambda_handler
        event = {
            'name': '',
            'this': 'next_step',
            'steps': {
                'next_step': {

                }
            }
        }
        context = {}
        lambda_handler(event, context)
