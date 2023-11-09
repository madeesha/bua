from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_copy_snapshot_same_account(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'restore_database',
            'data': {
                'params_id': 11,
                'snapshot_arn': 'arn:aws:rds:ap-southeast-2:123:snapshot:mydb-snapshot',
                'instance_type': '8xlarge',
                'mysql_version': '8.0.32',
                'instance_class': 'DBInstanceClassR6i'
            },
            'steps': {
                'restore_database': {
                    'action': 'copy_snapshot',
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
        assert body['result']['status'] == 'COMPLETE'

    def test_copy_snapshot_different_account(self, handler, sqs, update_id, suffix):
        body = {
            'name': 'Restore Database',
            'this': 'restore_database',
            'data': {
                'params_id': 11,
                'snapshot_arn': 'arn:aws:rds:ap-southeast-2:456:snapshot:mydb-snapshot',
                'instance_type': '8xlarge',
                'mysql_version': '8.0.32',
                'instance_class': 'DBInstanceClassR6i'
            },
            'steps': {
                'restore_database': {
                    'action': 'copy_snapshot',
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
        assert body['result']['status'] == 'COMPLETE'
