import pathlib
import os
from bua.facade.cf import CF
from bua.facade.rds import RDS
from bua.pipeline.handler.request import HandlerRequest


class Restore:

    def __init__(self, config, cf_client, rds: RDS):
        self.config = config
        self.cf = CF(cf_client)
        self.rds = rds
        self.prefix = self.config['prefix']
        self.cluster_name = self.config['cluster']
        self.env_name = self.config['env']

    def restore_database(self, request: HandlerRequest):
        data = request.data
        update_id = data['update_id']
        suffix = data['suffix']
        params_id = data['params_id']
        snapshot_arn = data['snapshot_arn']
        instance_type = data['instance_type']
        mysql_version = data['mysql_version']
        instance_class = data['instance_class']
        snapshot_account_id = snapshot_arn.split(':')[4]
        if snapshot_account_id != self.config['aws_account']:
            msg = f'Cannot perform cross account restore directly'
            return "NEEDCOPY", msg
        stack_name = f'{self.prefix}-{update_id}-{suffix}'
        stack = self.cf.check_stack_status(stack_name)
        if stack['StackStatus'] == 'CREATE_FAILED':
            msg = f'{stack_name} : Previous stack creation failed'
            return "FAILED", msg
        if stack['StackStatus'] == 'NO_SUCH_STACK':
            print(f'{stack_name} : Creating new stack using cf-rds-mysql.yml')
            template_path = pathlib.Path(__file__).parent / 'cf-rds-mysql.yml'
            if not os.path.exists(template_path):
                msg = f'{stack_name} : Cannot find template {template_path}'
                return "ABORT", msg
            self.cf.create_stack(
                stack_name, template_path,
                self.env_name, self.cluster_name,
                params_id, snapshot_arn, self.prefix, update_id, suffix,
                instance_type, mysql_version, instance_class)
        msg = f'{stack_name} : Stack creation in progress'
        return "COMPLETE", msg

    def check_restore_database(self, request: HandlerRequest):
        data = request.data
        update_id = data['update_id']
        suffix = data['suffix']
        stack_name = f'{self.config["prefix"]}-{update_id}-{suffix}'
        stack = self.cf.check_stack_status(stack_name)
        if stack['StackStatus'] == 'NO_SUCH_STACK':
            msg = f'{stack_name} : No such stack found'
            return "ABORT", msg
        elif stack['StackStatus'].endswith('_COMPLETE'):
            msg = f'{stack_name} : Restore database completed'
            return "COMPLETE", msg
        elif stack['StackStatus'].endswith('_FAILED'):
            msg = f'{stack_name} : Restore database failed'
            return "ABORT", msg
        msg = f'{stack_name} : Restore not complete yet'
        return "RETRY", msg

    def copy_snapshot(self, request: HandlerRequest):
        data = request.data
        snapshot_arn: str = data['snapshot_arn']
        kms_key_id: str = self.config['core_kms_key_id']
        mysql_version: str = data['mysql_version']
        if mysql_version.startswith('8.0'):
            option_group_name = self.config['mysql80_option_group_name']
        elif mysql_version.startswith('5.7'):
            option_group_name = self.config['mysql57_option_group_name']
        else:
            msg = f'{snapshot_arn} : Unsupported mysql version {mysql_version}'
            return "FAILED", msg
        snapshot_account_id = snapshot_arn.split(':')[4]
        if snapshot_account_id == self.config['aws_account']:
            msg = f'{snapshot_arn} : Already a local snapshot'
            return "COMPLETE", msg
        snapshot_name = snapshot_arn.split(':')[-1]
        snapshot_arn = self.rds.copy_snapshot(
            snapshot_arn=snapshot_arn, snapshot_name=snapshot_name,
            kms_key_id=kms_key_id, option_group_name=option_group_name
        )
        data['snapshot_arn'] = snapshot_arn
        msg = f'{snapshot_arn} : Copy in progress'
        return "COMPLETE", msg

    def check_copy_snapshot(self, request: HandlerRequest):
        data = request.data
        snapshot_arn = data['snapshot_arn']
        snapshot_name = snapshot_arn.split(':')[-1]
        status = self.rds.check_snapshot_status(snapshot_name=snapshot_name)
        if status == 'Available':
            msg = f'{snapshot_arn} : Status {status}'
            return "COMPLETE", msg
        msg = f'{snapshot_arn} : Status {status}'
        return "RETRY", msg
