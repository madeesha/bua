class RDSClientStub:
    def modify_db_instance(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'DBInstanceIdentifier', 'MasterUserPassword'}
        return {}

    def describe_db_snapshots(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'DBSnapshotIdentifier'}, key
        return {}

    def copy_db_snapshot(self, *args, **kwargs):
        _valid_keys = {'SourceDBSnapshotIdentifier', 'TargetDBSnapshotIdentifier', 'KmsKeyId', 'OptionGroupName'}
        for key in kwargs.keys():
            assert key in _valid_keys, key
        return {
            'DBSnapshot': {
                'DBSnapshotArn': f'arn:aws:rds:ap-southeast-2:123:snapshot:mydb-snapshot'
            }
        }
