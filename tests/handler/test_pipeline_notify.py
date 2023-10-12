from datetime import datetime
from zoneinfo import ZoneInfo


class TestCase:

    _environ = {
        'buaTableName': '',
        'failureQueueURL': '',
        'stateMachineArn': '',
        'updateId': '',
        'pipelineSteps': '',
    }

    def test_invoke_handler(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.pipeline_notify import lambda_handler
        event = {
            'Records': [
                {
                    'eventSource': 'aws:sqs',
                    'eventSourceARN': 'arn:123',
                    'messageId': '123',
                    'body': 'a:b:c:d',
                }
            ]
        }
        context = {}
        lambda_handler(event, context)
        monkey_patch.patch.stepfunctions().assert_n_start_executions(1)
        run_date = datetime.now(ZoneInfo('Australia/Sydney')).strftime('%Y-%m-%d')
        prefix = f'{run_date}-Notify-'
        monkey_patch.patch.stepfunctions().start_executions_startswith(0, 'name', prefix)
