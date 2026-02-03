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
        password = self.sm.fetch_secret(data['rdssecret'])['password']
        
        # Determine if this is Aurora or RDS based on snapshot ARN or explicit flag
        snapshot_arn = data.get('snapshot_arn', '')
        is_aurora = ':cluster-snapshot:' in snapshot_arn or data.get('database_type') == 'aurora'
        
        if is_aurora:
            # Aurora cluster
            db_cluster_identifier = f'{self.prefix}-{update_id}-{suffix}'
            print(f'{db_cluster_identifier} : reset master user password for Aurora cluster')
            self.rds.reset_cluster_password(db_cluster_identifier, password)
        else:
            # RDS instance
            db_instance_identifier = f'{self.prefix}-{update_id}-{suffix}'
            print(f'{db_instance_identifier} : reset master user password for RDS instance')
            self.rds.reset_password(db_instance_identifier, password)
        
        return "COMPLETE", 'Reset password completed'