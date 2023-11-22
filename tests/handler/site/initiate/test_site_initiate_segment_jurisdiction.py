import json
from datetime import datetime

from tests.handler.site.initiate import environ


class TestCase:

    def test_invoke_handler_run_type_segment_jurisdiction(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=environ)
        monkey_patch.patch.connect().cursor().add_result_set([
            {
                'jurisdiction_name': 'VIC',
                'res_bus': 'BUS',
                'stream_type': 'PRIMARY',
                'interval_date': datetime.strptime('2023-10-01', '%Y-%m-%d'),
            }
        ])
        from bua.handler.site_initiate import lambda_handler
        event = {
            'run_type': 'SegmentJurisdiction',
            'run_date': '2023-10-01',
            'source_date': '2023-10-01',
            'today': '2023-10-01',
            'start_inclusive': '2022-10-01',
            'end_inclusive': '2023-09-30',
            'end_exclusive': '2023-10-01',
            'identifier_type': 'Segment1',
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
        message = monkey_patch.patch.sqs().mysqs.get_queue('segment-queue').get_message()['message']
        assert monkey_patch.patch.sqs().mysqs.get_queue('segment-queue').get_message() is None
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
                    'avg_sum': 'Sum',
                    'identifier_type': 'Segment1',
                    'incl_est': False,
                    'interval_date': '20231001',
                    'jurisdiction_name': 'VIC',
                    'res_bus': 'BUS',
                    'run_date': '2023-10-01',
                    'run_type': 'SegmentJurisdiction',
                    'source_date': '2023-10-01',
                    'stream_type': 'PRIMARY'
                }
            ]
        }
