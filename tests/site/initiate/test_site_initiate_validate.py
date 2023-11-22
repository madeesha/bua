import json

from tests.site.initiate import environ


class TestCase:

    def test_invoke_handler_run_type_validate(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=environ)
        monkey_patch.patch.connect().cursor().add_result_set([
            {
                'nmi': '1234567890',
                'res_bus': 'BUS',
                'jurisdiction': 'VIC',
                'tni': 'ABC123',
                'nmi_suffix': 'E1',
                'stream_type': 'PRIMARY',
            }
        ])
        from bua.handler.site_initiate import lambda_handler
        event = {
            'run_type': 'Validate',
            'run_date': '2023-10-01',
            'source_date': '2023-10-01',
            'today': '2023-10-01',
            'start_inclusive': '2022-10-01',
            'end_inclusive': '2023-09-30',
            'end_exclusive': '2023-10-01',
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
        message = monkey_patch.patch.sqs().mysqs.get_queue('data-queue').get_message()['message']
        assert monkey_patch.patch.sqs().mysqs.get_queue('data-queue').get_message() is None
        assert json.loads(message) == {
            'db': {
                'prefix': 'tst',
                'update_id': '1',
                'suffix': 'sql',
                'domain': 'com',
                'schema': 'turkey',
            },
            'entries': [
                {
                    'end_exclusive': '2023-10-01',
                    'end_inclusive': '2023-09-30',
                    'jurisdiction': 'VIC',
                    'nmi': '1234567890',
                    'res_bus': 'BUS',
                    'run_date': '2023-10-01',
                    'run_type': 'Validate',
                    'source_date': '2023-10-01',
                    'start_inclusive': '2022-10-01',
                    'stream_types': {'E1': 'PRIMARY'},
                    'tni': 'ABC123',
                    'today': '2023-10-01'
                }
            ]
        }
