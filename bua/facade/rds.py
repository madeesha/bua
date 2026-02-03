from typing import Dict, Optional

from botocore.exceptions import ClientError


class RDS:

    def __init__(self, rds_client):
        self.rds = rds_client

    def reset_password(self, db_instance_identifier, password):
        """Reset password for RDS instance"""
        self.rds.modify_db_instance(DBInstanceIdentifier=db_instance_identifier, MasterUserPassword=password)

    def reset_cluster_password(self, db_cluster_identifier, password):
        """Reset password for Aurora cluster"""
        self.rds.modify_db_cluster(DBClusterIdentifier=db_cluster_identifier, MasterUserPassword=password)

    def copy_snapshot(self, snapshot_arn: str, snapshot_name: str, kms_key_id: str, option_group_name: str) \
            -> Optional[str]:

        snapshot_name = snapshot_name.lower()
        
        # Check if this is a cluster snapshot
        if ':cluster-snapshot:' in snapshot_arn:
            return self._copy_cluster_snapshot(snapshot_arn, snapshot_name, kms_key_id)
        else:
            return self._copy_db_snapshot(snapshot_arn, snapshot_name, kms_key_id, option_group_name)

    def _copy_db_snapshot(self, snapshot_arn: str, snapshot_name: str, kms_key_id: str, option_group_name: str) \
            -> Optional[str]:
        """Copy RDS instance snapshot"""
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

    def _copy_cluster_snapshot(self, snapshot_arn: str, snapshot_name: str, kms_key_id: str) -> Optional[str]:
        """Copy Aurora cluster snapshot"""
        snapshot = self.describe_db_cluster_snapshot(snapshot_name=snapshot_name)
        db_snapshot_identifier = snapshot.get('DBClusterSnapshotIdentifier')
        db_snapshot_arn = snapshot.get('DBClusterSnapshotArn')
        status = snapshot.get('Status')
        print(f'Cluster Snapshot {snapshot_name} , Id {db_snapshot_identifier} , Arn {db_snapshot_arn} , Status {status}')
        if db_snapshot_arn is not None:
            return db_snapshot_arn

        try:
            response = self.rds.copy_db_cluster_snapshot(
                SourceDBClusterSnapshotIdentifier=snapshot_arn,
                TargetDBClusterSnapshotIdentifier=snapshot_name,
                KmsKeyId=kms_key_id
            )
            return response['DBClusterSnapshot']['DBClusterSnapshotArn']
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBClusterSnapshotNotFoundFault':
                    return None
                else:
                    raise
            else:
                raise

    def check_snapshot_status(self, snapshot_name: str) -> str:
        """Check snapshot status - works for both RDS and Aurora"""
        snapshot_name = snapshot_name.lower()
        
        # Try cluster snapshot first
        snapshot = self.describe_db_cluster_snapshot(snapshot_name=snapshot_name)
        if snapshot['Status'] != 'NotFound':
            return snapshot['Status'].lower()
        
        # Fall back to instance snapshot
        snapshot = self.describe_db_snapshot(snapshot_name=snapshot_name)
        return snapshot['Status'].lower()

    def describe_db_snapshot(self, snapshot_name: str) -> Dict:
        """Describe RDS instance snapshot"""
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

    def describe_db_cluster_snapshot(self, snapshot_name: str) -> Dict:
        """Describe Aurora cluster snapshot"""
        try:
            snapshot_name = snapshot_name.lower()
            response = self.rds.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snapshot_name)
            if 'DBClusterSnapshots' in response:
                for snapshot in response['DBClusterSnapshots']:
                    if snapshot['DBClusterSnapshotIdentifier'] == snapshot_name:
                        return snapshot
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBClusterSnapshotNotFoundFault':
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

    def create_snapshot(self, snapshot_name, db_instance_identifier):
        """Create RDS instance snapshot"""
        snapshot_name = snapshot_name.lower()
        snapshot = self.describe_db_snapshot(snapshot_name=snapshot_name)
        db_snapshot_identifier = snapshot.get('DBSnapshotIdentifier')
        db_snapshot_arn = snapshot.get('DBSnapshotArn')
        status = snapshot.get('Status')
        print(f'Snapshot {snapshot_name} , Id {db_snapshot_identifier} , Arn {db_snapshot_arn} , Status {status}')
        if db_snapshot_arn is not None:
            return db_snapshot_arn

        try:
            response = self.rds.create_db_snapshot(
                DBSnapshotIdentifier=snapshot_name,
                DBInstanceIdentifier=db_instance_identifier
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

    def create_cluster_snapshot(self, snapshot_name, db_cluster_identifier):
        """Create Aurora cluster snapshot"""
        snapshot_name = snapshot_name.lower()
        snapshot = self.describe_db_cluster_snapshot(snapshot_name=snapshot_name)
        db_snapshot_identifier = snapshot.get('DBClusterSnapshotIdentifier')
        db_snapshot_arn = snapshot.get('DBClusterSnapshotArn')
        status = snapshot.get('Status')
        print(f'Cluster Snapshot {snapshot_name} , Id {db_snapshot_identifier} , Arn {db_snapshot_arn} , Status {status}')
        if db_snapshot_arn is not None:
            return db_snapshot_arn

        try:
            response = self.rds.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snapshot_name,
                DBClusterIdentifier=db_cluster_identifier
            )
            return response['DBClusterSnapshot']['DBClusterSnapshotArn']
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBClusterSnapshotNotFoundFault':
                    return None
                else:
                    raise
            else:
                raise