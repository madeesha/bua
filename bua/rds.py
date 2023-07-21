class RDS:

    def __init__(self, rds):
        self.rds = rds

    def reset_password(self, db_instance_identifier, password):
        self.rds.modify_db_instance(DBInstanceIdentifier=db_instance_identifier, MasterUserPassword=password)
