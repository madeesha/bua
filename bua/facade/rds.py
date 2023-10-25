class RDS:

    def __init__(self, rds_client):
        self.rds = rds_client

    def reset_password(self, db_instance_identifier, password):
        self.rds.modify_db_instance(DBInstanceIdentifier=db_instance_identifier, MasterUserPassword=password)

    def copy_snapshot(self, snapshot_arn: str, snapshot_name: str, kms_key_id: str, option_group_name: str):
        snapshot_id = f'rds:{snapshot_name}'
        response = self.rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id)
        if 'DBSnapshots' in response:
            for snapshot in response['DBSnapshots']:
                if snapshot['DBSnapshotIdentifier'] == snapshot_id:
                    return snapshot['DBSnapshotArn']
        response = self.rds.copy_db_snapshot(
            SourceDBSnapshotIdentifier=snapshot_arn,
            TargetDBSnapshotIdentifier=snapshot_name,
            KmsKeyId=kms_key_id,
            OptionGroupName=option_group_name
        )
        return response['DBSnapshot']['DBSnapshotArn']

    def check_snapshot_status(self, snapshot_name: str) -> str:
        snapshot_id = f'rds:{snapshot_name}'
        response = self.rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id)
        if 'DBSnapshots' in response:
            for snapshot in response['DBSnapshots']:
                if snapshot['DBSnapshotIdentifier'] == snapshot_id:
                    return snapshot['Status']
        return 'Unknown'
