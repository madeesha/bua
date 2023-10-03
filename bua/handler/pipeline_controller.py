import os
from bua.pipeline.handler.controller import BUAControllerHandler
import boto3
import botocore.config

session = boto3.session.Session()

sqs_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
sqs_client = boto3.client('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')

cf_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
cf_client = boto3.client('cloudformation', config=cf_config)

rds_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
rds_client = boto3.client('rds', config=rds_config)

sts_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
sts_client = boto3.client("sts", config=sts_config)

eks_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
eks_client = boto3.client('eks', config=eks_config)

ddb_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
ddb_resource = boto3.resource('dynamodb', config=ddb_config)
ddb_table = ddb_resource.Table(os.environ['tableName'])

s3_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
s3_client = boto3.client('s3', config=s3_config)

sm_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
sm_client = boto3.client('secretsmanager', config=sm_config)

r53_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
r53_client = boto3.client('route53', config=r53_config)

config = {
    'version': os.environ['projectVersion'],
    'prefix': os.environ['resourcePrefix'],
    'cluster': os.environ['className'],
    'env': os.environ['environmentName'],
    'region': os.environ['awsRegion'],
    'aws_account': os.environ['awsAccount'],
    'next_queue_url': os.environ['nextQueueURL'],
    'failure_queue_url': os.environ['failureQueueURL'],
    'initiate_queue_url': os.environ['initiateQueueURL'],
    'bucket_name': os.environ['bucketName']
}

handler = BUAControllerHandler(
    config=config, r53_client=r53_client, sm_client=sm_client, s3_client=s3_client,
    ddb_table=ddb_table, sqs_client=sqs_client, cf_client=cf_client, rds_client=rds_client,
    sts_client=sts_client, eks_client=eks_client, session=session
)


def lambda_handler(event, context):
    print(event)
    result = handler.handle_request(event)
    print(result)
    return result
