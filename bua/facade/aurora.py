from typing import Dict, Optional, List
import time

from botocore.exceptions import ClientError


class Aurora:
    """
    Aurora DB Cluster management facade
    Handles Aurora-specific operations that differ from standard RDS
    """

    def __init__(self, rds_client):
        self.rds = rds_client

    def reset_password(self, db_cluster_identifier: str, password: str):
        """Reset master password for Aurora cluster"""
        self.rds.modify_db_cluster(
            DBClusterIdentifier=db_cluster_identifier,
            MasterUserPassword=password,
            ApplyImmediately=True
        )

    def copy_snapshot(self, snapshot_arn: str, snapshot_name: str, kms_key_id: str) -> Optional[str]:
        """
        Copy Aurora cluster snapshot
        Note: Aurora snapshots don't use option groups
        """
        snapshot_name = snapshot_name.lower()
        snapshot = self.describe_db_cluster_snapshot(snapshot_name=snapshot_name)
        db_snapshot_identifier = snapshot.get('DBClusterSnapshotIdentifier')
        db_snapshot_arn = snapshot.get('DBClusterSnapshotArn')
        status = snapshot.get('Status')
        
        print(f'Aurora Snapshot {snapshot_name}, Id {db_snapshot_identifier}, Arn {db_snapshot_arn}, Status {status}')
        
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
        """Check Aurora cluster snapshot status"""
        snapshot_name = snapshot_name.lower()
        snapshot = self.describe_db_cluster_snapshot(snapshot_name=snapshot_name)
        return snapshot['Status'].lower()

    def describe_db_cluster_snapshot(self, snapshot_name: str) -> Dict:
        """Get Aurora cluster snapshot details"""
        try:
            snapshot_name = snapshot_name.lower()
            response = self.rds.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier=snapshot_name
            )
            if 'DBClusterSnapshots' in response:
                for snapshot in response['DBClusterSnapshots']:
                    if snapshot['DBClusterSnapshotIdentifier'] == snapshot_name:
                        return snapshot
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBClusterSnapshotNotFoundFault':
                    return {'Status': 'NotFound'}
                else:
                    raise
            else:
                raise
        return {'Status': 'Unknown'}

    def create_snapshot(self, snapshot_name: str, db_cluster_identifier: str) -> Optional[str]:
        """Create Aurora cluster snapshot"""
        snapshot_name = snapshot_name.lower()
        snapshot = self.describe_db_cluster_snapshot(snapshot_name=snapshot_name)
        db_snapshot_identifier = snapshot.get('DBClusterSnapshotIdentifier')
        db_snapshot_arn = snapshot.get('DBClusterSnapshotArn')
        status = snapshot.get('Status')
        
        print(f'Aurora Snapshot {snapshot_name}, Id {db_snapshot_identifier}, Arn {db_snapshot_arn}, Status {status}')
        
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

    def restore_cluster_from_snapshot(
        self,
        cluster_identifier: str,
        snapshot_arn: str,
        engine: str,
        engine_version: str,
        db_subnet_group_name: str,
        vpc_security_group_ids: List[str],
        db_cluster_parameter_group_name: str,
        kms_key_id: Optional[str] = None,
        tags: Optional[List[Dict]] = None
    ) -> Dict:
        """
        Restore Aurora cluster from snapshot
        Returns cluster info including status
        """
        try:
            params = {
                'DBClusterIdentifier': cluster_identifier,
                'SnapshotIdentifier': snapshot_arn,
                'Engine': engine,
                'EngineVersion': engine_version,
                'DBSubnetGroupName': db_subnet_group_name,
                'VpcSecurityGroupIds': vpc_security_group_ids,
                'DBClusterParameterGroupName': db_cluster_parameter_group_name,
                'EnableCloudwatchLogsExports': ['error', 'general', 'slowquery'],
                'DeletionProtection': False
            }
            
            if kms_key_id:
                params['KmsKeyId'] = kms_key_id
            
            if tags:
                params['Tags'] = tags
            
            response = self.rds.restore_db_cluster_from_snapshot(**params)
            
            return {
                'DBClusterArn': response['DBCluster']['DBClusterArn'],
                'Status': response['DBCluster']['Status'],
                'Endpoint': response['DBCluster'].get('Endpoint', ''),
                'ReaderEndpoint': response['DBCluster'].get('ReaderEndpoint', '')
            }
            
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                # Cluster already exists
                if 'Code' in error and error['Code'] == 'DBClusterAlreadyExistsFault':
                    cluster = self.describe_db_cluster(cluster_identifier)
                    return {
                        'DBClusterArn': cluster['DBClusterArn'],
                        'Status': cluster['Status'],
                        'Endpoint': cluster.get('Endpoint', ''),
                        'ReaderEndpoint': cluster.get('ReaderEndpoint', '')
                    }
                else:
                    raise
            else:
                raise

    def describe_db_cluster(self, cluster_identifier: str) -> Dict:
        """Get Aurora cluster details"""
        try:
            response = self.rds.describe_db_clusters(
                DBClusterIdentifier=cluster_identifier
            )
            if 'DBClusters' in response and len(response['DBClusters']) > 0:
                return response['DBClusters'][0]
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBClusterNotFoundFault':
                    return {'Status': 'NotFound'}
                else:
                    raise
            else:
                raise
        return {'Status': 'Unknown'}

    def check_cluster_status(self, cluster_identifier: str) -> Dict:
        """
        Check if Aurora cluster and all instances are available
        Returns detailed status information
        """
        cluster = self.describe_db_cluster(cluster_identifier)
        
        if cluster.get('Status') == 'NotFound':
            return {
                'cluster_status': 'NotFound',
                'instances_status': [],
                'is_available': False
            }
        
        cluster_status = cluster.get('Status', 'unknown')
        instances_status = []
        
        # Check all cluster members (instances)
        if 'DBClusterMembers' in cluster:
            for member in cluster['DBClusterMembers']:
                instance_id = member['DBInstanceIdentifier']
                try:
                    instance = self.describe_db_instance(instance_id)
                    instances_status.append({
                        'identifier': instance_id,
                        'status': instance.get('DBInstanceStatus', 'unknown'),
                        'is_writer': member.get('IsClusterWriter', False)
                    })
                except Exception as e:
                    instances_status.append({
                        'identifier': instance_id,
                        'status': 'error',
                        'error': str(e)
                    })
        
        # Check if everything is available
        is_available = (
            cluster_status == 'available' and
            len(instances_status) > 0 and
            all(inst['status'] == 'available' for inst in instances_status)
        )
        
        return {
            'cluster_status': cluster_status,
            'instances_status': instances_status,
            'is_available': is_available,
            'endpoint': cluster.get('Endpoint', ''),
            'reader_endpoint': cluster.get('ReaderEndpoint', '')
        }

    def describe_db_instance(self, instance_identifier: str) -> Dict:
        """Get Aurora instance details"""
        try:
            response = self.rds.describe_db_instances(
                DBInstanceIdentifier=instance_identifier
            )
            if 'DBInstances' in response and len(response['DBInstances']) > 0:
                return response['DBInstances'][0]
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBInstanceNotFound':
                    return {'DBInstanceStatus': 'NotFound'}
                else:
                    raise
            else:
                raise
        return {'DBInstanceStatus': 'Unknown'}

    def create_db_instance(
        self,
        instance_identifier: str,
        cluster_identifier: str,
        instance_class: str,
        engine: str,
        db_parameter_group_name: str,
        publicly_accessible: bool = False,
        tags: Optional[List[Dict]] = None
    ) -> Dict:
        """Create Aurora DB instance in cluster"""
        try:
            params = {
                'DBInstanceIdentifier': instance_identifier,
                'DBInstanceClass': instance_class,
                'Engine': engine,
                'DBClusterIdentifier': cluster_identifier,
                'DBParameterGroupName': db_parameter_group_name,
                'PubliclyAccessible': publicly_accessible
            }
            
            if tags:
                params['Tags'] = tags
            
            response = self.rds.create_db_instance(**params)
            
            return {
                'DBInstanceArn': response['DBInstance']['DBInstanceArn'],
                'Status': response['DBInstance']['DBInstanceStatus']
            }
            
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBInstanceAlreadyExists':
                    instance = self.describe_db_instance(instance_identifier)
                    return {
                        'DBInstanceArn': instance.get('DBInstanceArn', ''),
                        'Status': instance.get('DBInstanceStatus', 'unknown')
                    }
                else:
                    raise
            else:
                raise

    def delete_db_instance(self, instance_identifier: str, skip_final_snapshot: bool = True) -> Dict:
        """Delete Aurora DB instance"""
        try:
            response = self.rds.delete_db_instance(
                DBInstanceIdentifier=instance_identifier,
                SkipFinalSnapshot=skip_final_snapshot
            )
            return {
                'Status': response['DBInstance']['DBInstanceStatus']
            }
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBInstanceNotFound':
                    return {'Status': 'NotFound'}
                else:
                    raise
            else:
                raise

    def delete_db_cluster(self, cluster_identifier: str, skip_final_snapshot: bool = True) -> Dict:
        """Delete Aurora cluster"""
        try:
            response = self.rds.delete_db_cluster(
                DBClusterIdentifier=cluster_identifier,
                SkipFinalSnapshot=skip_final_snapshot
            )
            return {
                'Status': response['DBCluster']['Status']
            }
        except ClientError as ex:
            if 'Error' in ex.response:
                error = ex.response['Error']
                if 'Code' in error and error['Code'] == 'DBClusterNotFoundFault':
                    return {'Status': 'NotFound'}
                else:
                    raise
            else:
                raise

    def destroy_cluster_and_instances(self, cluster_identifier: str) -> Dict:
        """
        Delete all instances in cluster, then delete the cluster
        Returns the status of the operation
        """
        try:
            cluster = self.describe_db_cluster(cluster_identifier)
            
            if cluster.get('Status') == 'NotFound':
                return {'Status': 'NotFound', 'Message': 'Cluster already deleted'}
            
            # Delete all instances first
            deleted_instances = []
            if 'DBClusterMembers' in cluster:
                for member in cluster['DBClusterMembers']:
                    instance_id = member['DBInstanceIdentifier']
                    try:
                        self.delete_db_instance(instance_id, skip_final_snapshot=True)
                        deleted_instances.append(instance_id)
                    except Exception as e:
                        print(f"Error deleting instance {instance_id}: {e}")
            
            # Wait a moment for instances to start deleting
            time.sleep(5)
            
            # Delete the cluster
            result = self.delete_db_cluster(cluster_identifier, skip_final_snapshot=True)
            
            return {
                'Status': result['Status'],
                'DeletedInstances': deleted_instances
            }
            
        except Exception as e:
            return {
                'Status': 'Error',
                'Message': str(e)
            }

    def check_cluster_deletion_status(self, cluster_identifier: str) -> Dict:
        """Check if cluster is fully deleted"""
        cluster = self.describe_db_cluster(cluster_identifier)
        
        if cluster.get('Status') == 'NotFound':
            return {
                'is_deleted': True,
                'status': 'NotFound'
            }
        
        return {
            'is_deleted': False,
            'status': cluster.get('Status', 'unknown')
        }