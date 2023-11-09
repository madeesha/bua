from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_check_copy_snapshot(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'check_restore_database',
            'steps': {
                'check_restore_database': {
                    'action': 'check_copy_snapshot',
                    'args': {
                        'snapshot_arn': 'arn:aws:rds:ap-southeast-2:123:snapshot:mydb-snapshot'
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()

