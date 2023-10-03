import os
from bua.pipeline.handler.next import BUANextHandler
import boto3
import botocore.config

s3_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
s3_client = boto3.client('s3', config=s3_config)

sqs_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
sqs_client = boto3.client('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')

ddb_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
ddb_resource = boto3.resource('dynamodb', config=ddb_config)
ddb_table = ddb_resource.Table(os.environ['tableName'])

config = {
    'bucket_name': os.environ['bucketName']
}

handler = BUANextHandler(config=config, sqs=sqs_client, ddb=ddb_table, s3=s3_client)


def lambda_handler(event, context):
    print(event)
    handler.handle_request(event)
