import os
from bua.pipeline.handler.notify import BUANotifyHandler
import boto3
import botocore.config

sqs_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
sqs_client = boto3.client('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
sqs_resource = boto3.resource('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
failure_queue = sqs_resource.Queue(os.environ['failureQueueURL'])

ddb_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
ddb_resource = boto3.resource('dynamodb', config=ddb_config)
ddb_bua_table = ddb_resource.Table(os.environ['buaTableName'])

sfn_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
sfn_client = boto3.client('stepfunctions', config=sfn_config)

ssm_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
ssm_client = boto3.client('ssm', config=ssm_config)

config = {
    'prefix': os.environ['resourcePrefix'],
    'state_machine_arn': os.environ['stateMachineArn'],
    'aws_account_id': os.environ['awsAccountId'],
    'aws_region': os.environ['awsRegion']
}

handler = BUANotifyHandler(
    config=config,
    sqs_client=sqs_client, ddb_bua_table=ddb_bua_table, failure_queue=failure_queue, sfn_client=sfn_client,
    ssm_client=ssm_client
)


def lambda_handler(event, context):
    print(event)
    handler.handle_request(event)
