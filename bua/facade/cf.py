import os

import botocore.exceptions


class CF:

    def __init__(self, cf):
        self.cf = cf

    def check_change_set_status(self, stack_name, change_set_name):
        result = {
            'Status': 'NO_SUCH_CHANGE_SET',
            'StatusReason': 'Change set does not exist',
            'ExecutionStatus': 'UNAVAILABLE'
        }
        try:
            result = self.cf.describe_change_set(ChangeSetName=change_set_name, StackName=stack_name)
        except botocore.exceptions.ClientError as e:
            print(e)
            error = e.response.get('Error', {})
            if error.get('Code', {}) != 'ChangeSetNotFound':
                raise
            else:
                msg = error.get('Message', {})
                if msg is None or not msg.endswith('does not exist'):
                    raise
        if 'Status' not in result:
            result['Status'] = 'UNKNOWN'
        if 'StatusReason' not in result:
            result['StatusReason'] = ''
        if 'ExecutionStatus' not in result:
            result['ExecutionStatus'] = 'UNKNOWN'
        print(f'{stack_name} : {change_set_name} : {result["Status"]} : {result["StatusReason"]} : {result["ExecutionStatus"]}')
        return result

    def check_stack_status(self, stack_name):
        result = {
            'StackStatus': 'NO_SUCH_STACK',
            'StackStatusReason': 'Stack does not exist'
        }
        try:
            response = self.cf.describe_stacks(
                StackName=stack_name
            )
            if 'Stacks' in response:
                for stack in response['Stacks']:
                    result = stack
        except botocore.exceptions.ClientError as e:
            print(e)
            error = e.response.get('Error', {})
            if error.get('Code', {}) != 'ValidationError':
                raise
            else:
                msg = error.get('Message', {})
                if msg is None or not msg.endswith('does not exist'):
                    raise
        if 'StackStatus' not in result:
            result['StackStatus'] = 'UNKNOWN'
        if 'StackStatusReason' not in result:
            result['StackStatusReason'] = ''
        print(f'{stack_name} : {result["StackStatus"]} : {result["StackStatusReason"]}')
        return result

    def delete_stack(self, stack_name):
        self.cf.delete_stack(StackName=stack_name)

    def create_stack(
            self, stack_name, template_path,
            env_name, cluster_name,
            params_id, snapshot_arn, prefix, update_id, suffix,
            instance_type, mysql_version, instance_class
    ):
        print(f'Create stack with {template_path}')
        if not os.path.exists(template_path):
            raise Exception(f'Cannot find {template_path}')
        with open(template_path, 'r') as fp:
            template_body = '\n'.join(fp.readlines())
        if len(template_body) > 51200:
            raise Exception(f'Cannot use a template that exceeds 51200 bytes')
        self.cf.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=[
                {'ParameterKey': 'ParamsID', 'ParameterValue': params_id},
                {'ParameterKey': 'ResourcePrefix', 'ParameterValue': prefix},
                {'ParameterKey': 'ClassName', 'ParameterValue': cluster_name},
                {'ParameterKey': 'EnvironmentName', 'ParameterValue': env_name},
                {'ParameterKey': 'DBInstanceClass', 'ParameterValue': instance_class},
                {'ParameterKey': 'InstanceType', 'ParameterValue': instance_type},
                {'ParameterKey': 'SnapshotIdentifier', 'ParameterValue': snapshot_arn},
                {'ParameterKey': 'MySQLVersion', 'ParameterValue': mysql_version},
                {'ParameterKey': 'UpdateID', 'ParameterValue': update_id},
                {'ParameterKey': 'InstanceSuffix', 'ParameterValue': suffix},
            ],
            Capabilities=[
                'CAPABILITY_NAMED_IAM'
            ],
            DisableRollback=True
        )

    def create_aurora_stack(
            self, stack_name, template_path,
            env_name, cluster_name,
            params_id, snapshot_arn, prefix, update_id, suffix,
            instance_type, engine_version
    ):
        print(f'Create Aurora stack with {template_path}')
        if not os.path.exists(template_path):
            raise Exception(f'Cannot find {template_path}')
        with open(template_path, 'r') as fp:
            template_body = '\n'.join(fp.readlines())
        if len(template_body) > 51200:
            raise Exception(f'Cannot use a template that exceeds 51200 bytes')
        self.cf.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=[
                {'ParameterKey': 'ParamsID', 'ParameterValue': params_id},
                {'ParameterKey': 'ResourcePrefix', 'ParameterValue': prefix},
                {'ParameterKey': 'ClassName', 'ParameterValue': cluster_name},
                {'ParameterKey': 'EnvironmentName', 'ParameterValue': env_name},
                {'ParameterKey': 'InstanceType', 'ParameterValue': instance_type},
                {'ParameterKey': 'DBSnapshotIdentifier', 'ParameterValue': snapshot_arn},
                {'ParameterKey': 'EngineVersion', 'ParameterValue': engine_version},
                {'ParameterKey': 'UpdateID', 'ParameterValue': update_id},
                {'ParameterKey': 'BuildQlikCluster', 'ParameterValue': 'no'},
                {'ParameterKey': 'BuildAuditCluster', 'ParameterValue': 'no'},
            ],
            Capabilities=[
                'CAPABILITY_NAMED_IAM'
            ],
            DisableRollback=True
        )

    def create_upgrade_version_change_set(self, stack_name, change_set_name, template_path, mysql_version):
        print(f'Create change set with {template_path}')
        if not os.path.exists(template_path):
            raise Exception(f'Cannot find {template_path}')
        with open(template_path, 'r') as fp:
            template_body = '\n'.join(fp.readlines())
        if len(template_body) > 51200:
            raise Exception(f'Cannot use a template that exceeds 51200 bytes')
        self.cf.create_change_set(
            StackName=stack_name,
            ChangeSetName=change_set_name,
            ChangeSetType='UPDATE',
            TemplateBody=template_body,
            Parameters=[
                {'ParameterKey': 'ParamsID', 'UsePreviousValue': True},
                {'ParameterKey': 'ResourcePrefix', 'UsePreviousValue': True},
                {'ParameterKey': 'ClassName', 'UsePreviousValue': True},
                {'ParameterKey': 'EnvironmentName', 'UsePreviousValue': True},
                {'ParameterKey': 'DBInstanceClass', 'UsePreviousValue': True},
                {'ParameterKey': 'InstanceType', 'UsePreviousValue': True},
                {'ParameterKey': 'SnapshotIdentifier', 'UsePreviousValue': True},
                {'ParameterKey': 'MySQLVersion', 'ParameterValue': mysql_version},
                {'ParameterKey': 'UpdateID', 'UsePreviousValue': True},
                {'ParameterKey': 'InstanceSuffix', 'UsePreviousValue': True},
            ],
            Capabilities=[
                'CAPABILITY_NAMED_IAM'
            ]
        )

    def create_scale_change_set(self, stack_name, change_set_name, template_path, instance_type, instance_class):
        print(f'Create change set with {template_path}')
        if not os.path.exists(template_path):
            raise Exception(f'Cannot find {template_path}')
        with open(template_path, 'r') as fp:
            template_body = '\n'.join(fp.readlines())
        if len(template_body) > 51200:
            raise Exception(f'Cannot use a template that exceeds 51200 bytes')
        self.cf.create_change_set(
            StackName=stack_name,
            ChangeSetName=change_set_name,
            ChangeSetType='UPDATE',
            TemplateBody=template_body,
            Parameters=[
                {'ParameterKey': 'ParamsID', 'UsePreviousValue': True},
                {'ParameterKey': 'ResourcePrefix', 'UsePreviousValue': True},
                {'ParameterKey': 'ClassName', 'UsePreviousValue': True},
                {'ParameterKey': 'EnvironmentName', 'UsePreviousValue': True},
                {'ParameterKey': 'DBInstanceClass', 'ParameterValue': instance_class},
                {'ParameterKey': 'InstanceType', 'ParameterValue': instance_type},
                {'ParameterKey': 'SnapshotIdentifier', 'UsePreviousValue': True},
                {'ParameterKey': 'MySQLVersion', 'UsePreviousValue': True},
                {'ParameterKey': 'UpdateID', 'UsePreviousValue': True},
                {'ParameterKey': 'InstanceSuffix', 'UsePreviousValue': True},
            ],
            Capabilities=[
                'CAPABILITY_NAMED_IAM'
            ]
        )

    def create_change_set(
        self, stack_name, change_set_name, template_path,
        env_name, cluster_name,
        params_id, snapshot_arn, prefix, update_id, suffix,
        instance_type, mysql_version, instance_class
    ):
        print(f'Create change set with {template_path}')
        if not os.path.exists(template_path):
            raise Exception(f'Cannot find {template_path}')
        with open(template_path, 'r') as fp:
            template_body = '\n'.join(fp.readlines())
        if len(template_body) > 51200:
            raise Exception(f'Cannot use a template that exceeds 51200 bytes')
        self.cf.create_change_set(
            StackName=stack_name,
            ChangeSetName=change_set_name,
            ChangeSetType='UPDATE',
            TemplateBody=template_body,
            Parameters=[
                {'ParameterKey': 'ParamsID', 'ParameterValue': params_id},
                {'ParameterKey': 'ResourcePrefix', 'ParameterValue': prefix},
                {'ParameterKey': 'ClassName', 'ParameterValue': cluster_name},
                {'ParameterKey': 'EnvironmentName', 'ParameterValue': env_name},
                {'ParameterKey': 'DBInstanceClass', 'ParameterValue': instance_class},
                {'ParameterKey': 'InstanceType', 'ParameterValue': instance_type},
                {'ParameterKey': 'SnapshotIdentifier', 'ParameterValue': snapshot_arn},
                {'ParameterKey': 'MySQLVersion', 'ParameterValue': mysql_version},
                {'ParameterKey': 'UpdateID', 'ParameterValue': update_id},
                {'ParameterKey': 'InstanceSuffix', 'ParameterValue': suffix},
            ],
            Capabilities=[
                'CAPABILITY_NAMED_IAM'
            ]
        )

    def execute_change_set(self, stack_name, change_set_name):
        self.cf.execute_change_set(
            ChangeSetName=change_set_name,
            StackName=stack_name
        )