from datetime import datetime
from zoneinfo import ZoneInfo


class TestCase:

    _environ = {
        'buaTableName': '',
        'failureQueueURL': '',
        'stateMachineArn': '',
        'resourcePrefix': 'dev',
        'pipelineSteps': '',
        'sourceAccountId': '1234567890',
        'awsAccountId': '9876543210',
        'awsRegion': 'southeast-2',
    }

    def test_invoke_with_source_account_arn(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        monkey_patch.patch.client('ssm').parameters = {
            '/dev/bua/update_id': '100',
            '/dev/bua/snapshot_arn': '',
        }
        from bua.handler.pipeline_notify import lambda_handler
        snapshot_arn = 'arn:aws:rds:southeast-2:1234567890:snapshot:12345'
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'eventSourceARN': 'arn:123',
                    'messageId': '123',
                    'body': snapshot_arn,
                }
            ]
        }
        context = {}
        lambda_handler(event, context)
        monkey_patch.patch.stepfunctions().assert_n_start_executions(1)
        run_date = datetime.now(ZoneInfo('Australia/Sydney')).strftime('%Y-%m-%d')
        prefix = f'{run_date}-Notify-'
        monkey_patch.patch.stepfunctions().start_executions_startswith(0, 'name', prefix)
        assert monkey_patch.patch.ssm().parameters['/dev/bua/snapshot_arn'] == snapshot_arn

    def test_invoke_with_current_account_arn(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        monkey_patch.patch.client('ssm').parameters = {
            '/dev/bua/update_id': '100',
            '/dev/bua/snapshot_arn': '',
        }
        from bua.handler.pipeline_notify import lambda_handler
        snapshot_arn = 'arn:aws:rds:southeast-2:9876543210:snapshot:12345'
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'eventSourceARN': 'arn:123',
                    'messageId': '123',
                    'body': snapshot_arn,
                }
            ]
        }
        context = {}
        lambda_handler(event, context)
        monkey_patch.patch.stepfunctions().assert_n_start_executions(1)
        run_date = datetime.now(ZoneInfo('Australia/Sydney')).strftime('%Y-%m-%d')
        prefix = f'{run_date}-Notify-'
        monkey_patch.patch.stepfunctions().start_executions_startswith(0, 'name', prefix)
        assert monkey_patch.patch.ssm().parameters['/dev/bua/snapshot_arn'] == snapshot_arn

    def test_invoke_with_another_account_arn(self):
        import tests.monkey.patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        monkey_patch.patch.client('ssm').parameters = {
            '/dev/bua/update_id': '100',
            '/dev/bua/snapshot_arn': '',
        }
        from bua.handler.pipeline_notify import lambda_handler
        snapshot_arn = 'arn:aws:rds:southeast-2:1111111111:snapshot:12345'
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'eventSourceARN': 'arn:123',
                    'messageId': '123',
                    'body': snapshot_arn,
                }
            ]
        }
        context = {}
        lambda_handler(event, context)
        monkey_patch.patch.stepfunctions().assert_n_start_executions(0)
        assert monkey_patch.patch.ssm().parameters['/dev/bua/snapshot_arn'] == ''
