from bua.facade.rds import RDS
from bua.facade.sm import SecretManager
from bua.pipeline.handler.request import HandlerRequest


class Reset:

    def __init__(self, config, rds: RDS, secret_manager: SecretManager):
        self.config = config
        self.rds = rds
        self.sm = secret_manager
        self.prefix = config['prefix']

    def reset_password(self, request: HandlerRequest):
        data = request.data
        update_id = data['update_id']
        suffix = data['suffix']
        db_instance_identifier = f'{self.prefix}-{update_id}-{suffix}'
        password = self.sm.fetch_secret(data['rdssecret'])['password']
        print(f'{db_instance_identifier} : reset master user password')
        self.rds.reset_password(db_instance_identifier, password)
        return "COMPLETE", 'Reset password completed'
