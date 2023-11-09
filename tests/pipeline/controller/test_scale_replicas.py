from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_scale_replicas(self, handler, sqs):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
            },
            'steps': {
                'step1': {
                    'action': 'scale_replicas',
                    'args': {
                        'deployment': [ 'workflow' ],
                        'namespace': 'core',
                        'replicas': 2
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
