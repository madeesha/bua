from tests.site.initiate import environ


class TestCase:

    def test_invoke_handler_run_type_requeue(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=environ)
        monkey_patch.patch.mysqs.get_queue('source-queue').put_message('my-message')
        from bua.handler.site_initiate import lambda_handler
        event = {
            'run_type': 'Requeue',
            'run_date': '2023-10-01',
            'source_date': '2023-10-01',
            'today': '2023-10-01',
            'start_inclusive': '2022-10-01',
            'end_inclusive': '2023-09-30',
            'end_exclusive': '2023-10-01',
            'source_queue': 'source-queue',
            'target_queue': 'target-queue',
            'db': {
                'prefix': 'tst',
                'update_id': '1',
                'suffix': 'sql',
                'domain': 'com',
                'schema': 'turkey',
            }
        }
        context = {}
        lambda_handler(event, context)
        assert monkey_patch.patch.sqs().mysqs.get_queue('failure-queue').get_message() is None
        message = monkey_patch.patch.sqs().mysqs.get_queue('target-queue').get_message()['message']
        assert monkey_patch.patch.sqs().mysqs.get_queue('target-queue').get_message() is None
        assert message == 'my-message'
