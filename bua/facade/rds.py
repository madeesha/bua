from typing import Dict, Optional

from botocore.exceptions import ClientError


class RDS:

    def __init__(self, rds_client):
        self.rds = rds_client

    def reset_password(self, db_instance_identifier, password):
        self.rds.modify_db_instance(DBInstanceIdentifier=db_instance_identifier, MasterUserPassword=password)

    def copy_snapshot(self, snapshot_arn: str, snapshot_name: str, kms_key_id: str, option_group_name: str) \
            -> Optional[str]:
        snapshot_id = f'rds:{snapshot_name}'
        snapshot = self._describe_db_snapshot(snapshot_id=snapshot_id)
        if 'DBSnapshotArn' in snapshot:
            return snapshot['DBSnapshotArn']
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
        snapshot_id = f'rds:{snapshot_name}'
        snapshot = self._describe_db_snapshot(snapshot_id=snapshot_id)
        return snapshot['Status']

    def _describe_db_snapshot(self, snapshot_id: str) -> Dict:
        try:
            response = self.rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id)
            if 'DBSnapshots' in response:
                for snapshot in response['DBSnapshots']:
                    if snapshot['DBSnapshotIdentifier'] == snapshot_id:
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
