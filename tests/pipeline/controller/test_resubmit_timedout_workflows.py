from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_resubmit_timedout_workflows(self, handler, sqs, rds_secret_id, sql_secret_id, update_id, suffix, rds_domain_name, schema_name, mysql, print):
        mysql.resultsets = [
            [
                {
                    'id': 1  # workflow 1
                }
            ],
            [
                {
                    'id': 2  # workflow 2
                }
            ]
        ]
        mysql.affectedrows = [
            10, 20
        ]
        body = {
            'name': 'Test',
            'this': 'step1',
            'data': {
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
                'schema': schema_name,
                'rdssecret': rds_secret_id,
                'sqlsecret': sql_secret_id
            },
            'steps': {
                'step1': {
                    'action': 'resubmit_timedout_workflows',
                    'args': {
                        'workflow_names': [
                            'Workflow1',
                            'Workflow2'
                        ]
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
        print.assert_has_line('Invoking [resubmit_timedout_workflows] for [step1]')
        print.assert_has_line('Resubmitted 10 Workflow1 workflow instances')
        print.assert_has_line('Resubmitted 20 Workflow2 workflow instances')
