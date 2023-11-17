import pytest


class TestCase:

    _environ = {
        'buaTableName': 'bua-table',
        'exportQueueURL': 'export-queue',
        'failureQueueURL': 'failure-queue',
        'rdsSecretName': 'rds-secret',
        'debugEnabled': 'Yes',
        'buaBucketName': 'bua-bucket',
        'maxReceiveCount': '100',
    }

    def test_invoke_handler(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        monkey_patch.patch.connect().cursor().add_result_set([
            {
                'Field1': 'A',
                'Field2': 'B'
            }
        ])
        monkey_patch.patch.connect().cursor().description = [
            ['Field1'], ['Field2']
        ]
        from bua.handler.site_export import lambda_handler
        event = {
            'run_type': 'ExportTables',
            'run_date': '2023-10-02',
            'today': '2023-10-01',
            'current_date': '2023-11-01',
            'current_time': '10:11:12',
            'bucket_name': 'my-bucket',
            'bucket_prefix': 'output/path/',
            'file_format': 'csv',
            'identifier_type': 'segment-one',
            'table_name': 'my_table',
            'partition': None,
            'counter': 1,
            'offset': 0,
            'batch_size': 100,
            'db': {
                'prefix': 'tst',
                'update_id': '1',
                'suffix': 'sql',
                'domain': 'com',
                'schema': 'turkey'
            }
        }
        context = {}
        result = lambda_handler(event, context)
        assert result == {
            'status': 'DONE'
        }
        queue = monkey_patch.patch.mysqs.get_queue('failure-queue')
        assert len(queue.messages) == 0
        bucket = monkey_patch.patch.mys3.get_bucket('my-bucket')
        assert len(bucket.objects) == 1
        assert list(bucket.objects.keys())[0] == 'output/path/BUA_my_table_20231101_101112_00001.csv'

    def test_invoke_handler_failure(self):
        with pytest.raises(KeyError):
            import tests.monkey.patch as monkey_patch
            monkey_patch.patch.patch(environ=self._environ)
            from bua.handler.site_export import lambda_handler
            event = {
                'Records': [
                    {
                        'eventSource': 'aws:sqs'
                    }
                ]
            }
            context = {}
            lambda_handler(event, context)
