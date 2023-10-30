from typing import Dict, Optional

from botocore.exceptions import ClientError


class RDS:

    def __init__(self, rds_client):
        self.rds = rds_client

    def reset_password(self, db_instance_identifier, password):
        self.rds.modify_db_instance(DBInstanceIdentifier=db_instance_identifier, MasterUserPassword=password)

    def copy_snapshot(self, snapshot_arn: str, snapshot_name: str, kms_key_id: str, option_group_name: str) \
            -> Optional[str]:

        snapshot_name = snapshot_name.lower()
        snapshot = self.describe_db_snapshot(snapshot_name=snapshot_name)
        db_snapshot_identifier = snapshot.get('DBSnapshotIdentifier')
        db_snapshot_arn = snapshot.get('DBSnapshotArn')
        status = snapshot.get('Status')
        print(f'Snapshot {snapshot_name} , Id {db_snapshot_identifier} , Arn {db_snapshot_arn} , Status {status}')
        if db_snapshot_arn is not None:
            return db_snapshot_arn

        try:
            response = self.rds.copy_db_snapshot(
                SourceDBSnapshotIdentifier=snapshot_arn,
                TargetDBSnapshotIdentifier=snapshot_name,
                KmsKeyId=kms_key_id,
                OptionGroupName=option_group_name
            )
            return response['DBSnapshot']['DBSnapshotArn']
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBSnapshotNotFound':
                    return None
                else:
                    raise
            else:
                raise

    def check_snapshot_status(self, snapshot_name: str) -> str:
        snapshot_name = snapshot_name.lower()
        snapshot = self.describe_db_snapshot(snapshot_name=snapshot_name)
        return snapshot['Status'].lower()

    def describe_db_snapshot(self, snapshot_name: str) -> Dict:
        try:
            snapshot_name = snapshot_name.lower()
            response = self.rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot_name)
            if 'DBSnapshots' in response:
                for snapshot in response['DBSnapshots']:
                    if snapshot['DBSnapshotIdentifier'] == snapshot_name:
                        return snapshot
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBSnapshotNotFound':
                    return {
                        'Status': 'NotFound'
                    }
                else:
                    raise
            else:
                raise
        return {
            'Status': 'Unknown'
        }
