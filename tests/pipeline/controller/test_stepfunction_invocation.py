from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_stepfunction_invocation(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'restore_database',
            'type': 'step',
            'data': {
                'params_id': 11,
                'snapshot_arn': 'arn:aws:rds:ap-southeast-2:123:snapshot:mydb-snapshot',
                'instance_type': '8xlarge',
                'mysql_version': '8.0.35',
                'instance_class': 'DBInstanceClassR6i'
            },
            'steps': {
                'restore_database': {
                    'action': 'restore_database',
                    'args': {
                        'update_id': update_id,
                        'suffix': suffix
                    },
                    'on': {
                        'COMPLETE': {
                            'next': 'next_step',
                            'delay': 120,
                        }
                    }
                },
                'next_step': {
                    'comment': 'Do something',
                    'speed': 'slow'
                }
            }
        }
        event = handler.handle_request(body)
        sqs.assert_no_failures()
        sqs.assert_no_messages()
        assert event['next'] == 'next_step'
        assert event['speed'] == 'slow'
        assert event['delay'] == 120
        assert event['type'] == 'step'
