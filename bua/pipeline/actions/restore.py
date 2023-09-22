import pathlib
import os
from bua.pipeline.facade.cf import CF
from bua.pipeline.handler.request import HandlerRequest


class Restore:

    def __init__(self, config, cf_client):
        self.config = config
        self.cf = CF(cf_client)
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
