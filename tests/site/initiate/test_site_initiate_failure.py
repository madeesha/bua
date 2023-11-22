import pytest

from tests.site.initiate import environ


class TestCase:

    def test_invoke_handler_failure(self):
        with pytest.raises(KeyError):
            import tests.monkey.patch as monkey_patch
            monkey_patch.patch.patch(environ=environ)
            from bua.handler.site_initiate import lambda_handler
            event = {
                'Records': [
                    {
                        'eventSource': 'aws:sqs'
                    }
                ]
            }
            context = {}
            lambda_handler(event, context)
