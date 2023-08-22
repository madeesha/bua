from bua.pipeline.facade.cf import CF


class Destroy:

    def __init__(self, config, cf_client):
        self.config = config
        self.cf = CF(cf_client)
        self.prefix = self.config['prefix']
        self.cluster_name = self.config['cluster']
        self.env_name = self.config['env']

    def destroy_database(self, step, data):
        update_id = data['update_id']
        suffix = data['suffix']
        stack_name = f'{self.prefix}-{update_id}-{suffix}'
        stack = self.cf.check_stack_status(stack_name)
        if stack['StackStatus'] == 'NO_SUCH_STACK':
            return "COMPLETE", 'Stack does not exist'
        if stack['StackStatus'].endswith('_COMPLETE') or stack['StackStatus'].endswith('_FAILED'):
            print(f'{stack_name} : Delete stack')
            self.cf.delete_stack(stack_name)
            msg = f'{stack_name} : Deletion of stack started'
            return "COMPLETE", msg
        msg = f'{stack_name} : Stack is not in a state that can be automatically deleted'
        return "ABORT", msg

    def check_destroy_database(self, step, data):
        update_id = data['update_id']
        suffix = data['suffix']
        stack_name = f'{self.config["prefix"]}-{update_id}-{suffix}'
        stack = self.cf.check_stack_status(stack_name)
        if stack['StackStatus'] == 'NO_SUCH_STACK':
            msg = f'{stack_name} : Stack has been destroyed'
            return "COMPLETE", msg
        elif stack['StackStatus'].endswith('_COMPLETE'):
            msg = f'{stack_name} : Has not been destroyed, it is still in a completed state. Destroy manually'
            return "ABORT", msg
        elif stack['StackStatus'].endswith('_FAILED'):
            msg = f'{stack_name} : Has not been destroyed, it is still in a failed state. Destroy manually'
            return "ABORT", msg
        msg = f'{stack_name} : Stack is still in the process of being deleted'
        return "RETRY", msg
