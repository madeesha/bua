from tests.handler.site.initiate import environ


class TestCase:

    def test_invoke_handler_entries(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=environ)
        from bua.handler.site_initiate import lambda_handler
        event = {
            'entries': [

            ]
        }
        context = {}
        lambda_handler(event, context)
        assert len(monkey_patch.patch.sqs().mysqs.get_queue('failure-queue').messages) == 0
