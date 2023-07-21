import os
import pathlib

from bua.cf import CF


class ChangeSet:

    def __init__(self, config, cf):
        self.config = config
        self.cf = CF(cf)
        self.prefix = self.config['prefix']
        self.cluster_name = self.config['cluster']
        self.env_name = self.config['env']

    def create_upgrade_version_change_set(self, step, data):
        update_id = data['update_id']
        suffix = data['suffix']
        mysql_version = data['mysql_version']
        stack_name = f'{self.prefix}-{update_id}-{suffix}'
        stack = self.cf.check_stack_status(stack_name)
        if not stack['StackStatus'].endswith('_COMPLETE'):
            msg = f'{stack_name} : Stack progress [{stack["StackStatus"]}] is not complete'
            return "FAILED", msg
        change_set_name = data['change_set_name']
        change_set = self.cf.check_change_set_status(stack_name, change_set_name)
        if change_set['Status'] == 'NO_SUCH_CHANGE_SET':
            print(f'{stack_name} : {change_set_name} : Creating change set using cf-rds-mysql.yml')
            template_path = pathlib.Path(__file__).parent / 'cf-rds-mysql.yml'
            if not os.path.exists(template_path):
                msg = f'{stack_name} : Cannot find template {template_path}'
                return "ABORT", msg
            self.cf.create_upgrade_version_change_set(stack_name, change_set_name, template_path, mysql_version)
        msg = f'{stack_name} : {change_set_name} : Change set creation in progress'
        return "COMPLETE", msg

    def create_scale_change_set(self, step, data):
        update_id = data['update_id']
        suffix = data['suffix']
        instance_type = data['instance_type']
        instance_class = data['instance_class']
        stack_name = f'{self.prefix}-{update_id}-{suffix}'
        stack = self.cf.check_stack_status(stack_name)
        if not stack['StackStatus'].endswith('_COMPLETE'):
            msg = f'{stack_name} : Stack progress [{stack["StackStatus"]}] is not complete'
            return "FAILED", msg
        change_set_name = data['change_set_name']
        change_set = self.cf.check_change_set_status(stack_name, change_set_name)
        if change_set['Status'] == 'NO_SUCH_CHANGE_SET':
            print(f'{stack_name} : {change_set_name} : Creating change set using cf-rds-mysql.yml')
            template_path = pathlib.Path(__file__).parent / 'cf-rds-mysql.yml'
            if not os.path.exists(template_path):
                msg = f'{stack_name} : Cannot find template {template_path}'
                return "ABORT", msg
            self.cf.create_scale_change_set(stack_name, change_set_name, template_path, instance_type, instance_class)
        msg = f'{stack_name} : {change_set_name} : Change set creation in progress'
        return "COMPLETE", msg

    def create_change_set(self, step, data):
        update_id = data['update_id']
        suffix = data['suffix']
        params_id = data['params_id']
        snapshot_arn = data['snapshot_arn']
        instance_type = data['instance_type']
        mysql_version = data['mysql_version']
        instance_class = data['instance_class']
        stack_name = f'{self.prefix}-{update_id}-{suffix}'
        stack = self.cf.check_stack_status(stack_name)
        if not stack['StackStatus'].endswith('_COMPLETE'):
            msg = f'{stack_name} : Stack progress [{stack["StackStatus"]}] is not complete'
            return "FAILED", msg
        change_set_name = data['change_set_name']
        change_set = self.cf.check_change_set_status(stack_name, change_set_name)
        if change_set['Status'] == 'NO_SUCH_CHANGE_SET':
            print(f'{stack_name} : {change_set_name} : Creating change set using cf-rds-mysql.yml')
            template_path = pathlib.Path(__file__).parent / 'cf-rds-mysql.yml'
            if not os.path.exists(template_path):
                msg = f'{stack_name} : Cannot find template {template_path}'
                return "ABORT", msg
            self.cf.create_change_set(
                stack_name, change_set_name, template_path,
                self.env_name, self.cluster_name,
                params_id, snapshot_arn, self.prefix, update_id, suffix,
                instance_type, mysql_version, instance_class)
        msg = f'{stack_name} : {change_set_name} : Change set creation in progress'
        return "COMPLETE", msg

    def check_change_set_ready(self, step, data):
        update_id = data['update_id']
        suffix = data['suffix']
        stack_name = f'{self.prefix}-{update_id}-{suffix}'
        change_set_name = data['change_set_name']
        change_set = self.cf.check_change_set_status(stack_name, change_set_name)
        msg = f'{stack_name} : {change_set_name} : Change set [{change_set["Status"]}] {change_set["StatusReason"]}'
        if 'NO_SUCH_CHANGE_SET' in change_set['Status']:
            return "ABORT", msg
        elif 'FAILED' in change_set['Status'] and "Submit different information" in change_set['StatusReason']:
            return "COMPLETE", msg
        elif 'FAILED' in change_set['Status']:
            return "ABORT", msg
        elif 'COMPLETE' in change_set['Status'] and change_set['ExecutionStatus'] == 'AVAILABLE':
            return "COMPLETE", msg
        elif 'COMPLETE' in change_set['Status'] and change_set['ExecutionStatus'] == 'OBSOLETE':
            return "COMPLETE", msg
        elif 'COMPLETE' in change_set['Status'] and 'COMPLETE' in change_set['ExecutionStatus']:
            return "COMPLETE", msg
        elif 'COMPLETE' in change_set['Status'] and 'FAILED' in change_set['ExecutionStatus']:
            return "COMPLETE", msg
        return "RETRY", msg

    def check_change_set_complete(self, step, data):
        update_id = data['update_id']
        suffix = data['suffix']
        stack_name = f'{self.prefix}-{update_id}-{suffix}'
        change_set_name = data['change_set_name']
        stack = self.cf.check_stack_status(stack_name)
        stack_status = stack['StackStatus']
        stack_status_reason = stack['StackStatusReason']
        if stack_status == 'NO_SUCH_STACK':
            msg = f'{stack_name} : No such stack found'
            return "ABORT", msg
        elif stack_status.endswith('_COMPLETE'):
            msg = f'{stack_name} : {change_set_name} : {stack_status} : Change set completed. {stack_status_reason}'
            return "COMPLETE", msg
        elif stack_status.endswith('_FAILED'):
            msg = f'{stack_name} : {change_set_name} : {stack_status} : Change set failed. {stack_status_reason}'
            return "ABORT", msg
        msg = f'{stack_name} : {change_set_name} : {stack_status} : Change set not complete yet. {stack_status_reason}'
        return "RETRY", msg

    def execute_change_set(self, step, data):
        update_id = data['update_id']
        suffix = data['suffix']
        stack_name = f'{self.prefix}-{update_id}-{suffix}'
        change_set_name = data['change_set_name']
        change_set = self.cf.check_change_set_status(stack_name, change_set_name)
        if change_set['ExecutionStatus'] == 'AVAILABLE':
            self.cf.execute_change_set(stack_name, change_set_name)
        msg = f'{stack_name} : {change_set_name} : Change set [{change_set["ExecutionStatus"]}]'
        return "COMPLETE", msg
