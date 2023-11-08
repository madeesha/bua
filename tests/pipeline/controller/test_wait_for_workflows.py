from pytest import mark

from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    @mark.parametrize("params", [
        {
            'resultsets': [[{'id': 123}], [{'status': 'NEW', 'total': 100}]],
            'status': 'RETRY'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'NEW', 'total': 100}]],
            'max_new': 100,
            'status': 'COMPLETE'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'READY', 'total': 100}]],
            'status': 'RETRY'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'READY', 'total': 100}]],
            'max_ready': 100,
            'status': 'COMPLETE'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'INPROG', 'total': 100}]],
            'status': 'RETRY'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'INPROG', 'total': 100}]],
            'max_inprog': 100,
            'status': 'COMPLETE'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'DONE', 'total': 100}]],
            'status': 'COMPLETE'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'ERROR', 'total': 100}]],
            'status': 'FAILED'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'ERROR', 'total': 100}]],
            'max_errors': 100,
            'status': 'COMPLETE'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'EXIT', 'total': 100}]],
            'status': 'FAILED'
        },
        {
            'resultsets': [[{'id': 123}], [{'status': 'EXIT', 'total': 100}]],
            'max_exit': 100,
            'status': 'COMPLETE'
        },
    ])
    def test_wait_for_workflows(self, handler, sqs, mysql, rds_secret_id, update_id, suffix, rds_domain_name, schema_name, params):
        mysql.resultsets = params['resultsets']
        body = {
            'name': 'Run a Test',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id
            },
            'steps': {
                'step1': {
                    'action': 'wait_for_workflows',
                    'args': {
                        'max_errors': params.get('max_errors', 0),
                        'max_hold': params.get('max_hold', 0),
                        'max_new': params.get('max_new', 0),
                        'max_ready': params.get('max_ready', 0),
                        'max_inprog': params.get('max_inprog', 0),
                        'max_exit': params.get('max_exit', 0),
                        'workflow_names': ['ExecuteSQL']
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
        assert body['result']['status'] == params['status']
