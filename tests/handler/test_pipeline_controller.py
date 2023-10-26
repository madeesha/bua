from botocore.exceptions import ClientError


class TestCase:

    _environ = {
        'buaTableName': '',
        'projectVersion': '',
        'resourcePrefix': '',
        'className': '',
        'environmentName': '',
        'awsRegion': '',
        'awsAccount': '',
        'nextQueueURL': '',
        'failureQueueURL': '',
        'initiateQueueURL': '',
        'bucketName': '',
        'mysql80optionGroupName': '',
        'coreKmsKeyId': '',
    }

    def test_invoke_handler(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.pipeline_controller import lambda_handler
        event = {
            'name': '',
            'this': ''
        }
        context = {}
        lambda_handler(event, context)

    def test_get_config(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.pipeline_controller import lambda_handler
        event = {
            'action': 'get_config'
        }
        context = {}
        lambda_handler(event, context)

    def test_create_upgrade_version_change_set(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        monkey_patch.patch.cloudformation().describe_stacks_responses(
            {
                'Stacks': [
                    {
                        'StackStatus': 'CREATE_COMPLETE',
                    }
                ]
            }
        )
        monkey_patch.patch.cloudformation().describe_change_set_responses(
            ClientError(
                error_response={
                    'Error': {
                        'Code': 'ChangeSetNotFound',
                        'Message': 'does not exist'
                    }
                },
                operation_name='describe_change_set'
            )
        )
        monkey_patch.patch.cloudformation().create_change_set_responses(
            {

            }
        )
        from bua.handler.pipeline_controller import lambda_handler
        event = {
            'action': 'create_upgrade_version_change_set',
            'args': {
                'update_id': 1,
                'suffix': '',
                'mysql_version': '8.0',
                'change_set_name': '',
            }
        }
        context = {}
        lambda_handler(event, context)

    def test_bua_create_macro_profile(self):
        import tests.handler.monkey_patch as monkey_patch
        monkey_patch.patch.patch(environ=self._environ)
        from bua.handler.pipeline_controller import lambda_handler
        event = {
            'action': 'bua_create_macro_profile',
            'args': {
                'update_id': 1,
                'suffix': '',
                'mysql_version': '8.0',
                'domain': '',
                'schema': '',
                'rdssecret': '',
                'identifier_type': 'SegmentJurisdictionAvgExclEst',
                "stream_types": ["CONTROL", "PRIMARY", "SOLAR", "UNKNOWN", "GAS"]
            }
        }
        context = {}
        lambda_handler(event, context)
        monkey_patch.patch.sqs().assert_no_messages()
        monkey_patch.patch.connect().cursor().assert_n_execute_invocations(6)
