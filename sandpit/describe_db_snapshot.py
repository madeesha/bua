import boto3
import botocore.config
import yaml

from bua.facade.rds import RDS

if __name__ == '__main__':
    rds_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
    rds_session = boto3.session.Session(region_name='ap-southeast-2', profile_name='matten')
    rds_client = rds_session.client('rds', config=rds_config)
    rds = RDS(rds_client=rds_client)
    snapshot_arn = 'arn:aws:rds:ap-southeast-2:760694178318:snapshot:prd-earl-1-sql-05-39-Oct-30-2023-shared-key-encrypted'
    snapshot_name = snapshot_arn.split(':')[-1]
    print(snapshot_name)
    print(yaml.dump(rds.describe_db_snapshot(snapshot_name=snapshot_name)))
