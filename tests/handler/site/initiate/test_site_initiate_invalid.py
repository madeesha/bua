import json

from tests.handler.site.initiate import environ


class TestCase:

    def test_invoke_handler_run_type_invalid(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=environ)
        from bua.handler.site_initiate import lambda_handler
        event = {
            'run_type': 'blah'
        }
        context = {}
        lambda_handler(event, context)
        assert len(monkey_patch.patch.sqs().mysqs.get_queue('failure-queue').messages) == 1
        values = monkey_patch.patch.sqs().mysqs.get_queue('failure-queue').messages.values()
        cause = json.loads(list(values)[0])['cause']
        assert cause == 'Do not know how to handle run_type blah'
