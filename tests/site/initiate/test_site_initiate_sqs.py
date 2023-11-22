from tests.site.initiate import environ


class TestCase:

    def test_invoke_handler_sqs(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=environ)
        from bua.handler.site_initiate import lambda_handler
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'eventSourceARN': 'arn:aws',
                    'messageId': '123',
                    'body': ''
                }
            ]
        }
        context = {}
        lambda_handler(event, context)
        assert len(monkey_patch.patch.sqs().mysqs.get_queue('failure-queue').messages) == 0

    def test_invoke_handler_sqs_too_many_retries(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=environ)
        from bua.handler.site_initiate import lambda_handler
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'eventSourceARN': 'arn:aws',
                    'messageId': '123',
                    'body': '',
                    'attributes': {
                        'ApproximateReceiveCount': '100'
                    }
                }
            ]
        }
        context = {}
        lambda_handler(event, context)
        assert len(monkey_patch.patch.sqs().mysqs.get_queue('failure-queue').messages) == 0
