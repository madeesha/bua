import os
from bua.site.handler.exporter import BUASiteExportHandler
import boto3
import botocore.config
import json
import pymysql
import pymysql.cursors
import traceback

ddb_config = botocore.config.Config(max_pool_connections=10, connect_timeout=10, read_timeout=30)
ddb = boto3.resource('dynamodb', config=ddb_config)
ddb_bua_table = ddb.Table(os.environ['buaTableName'])

sqs_config = botocore.config.Config(max_pool_connections=10, connect_timeout=10, read_timeout=30)
sqs = boto3.resource('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
sqs_client = boto3.client('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
export_queue = sqs.Queue(os.environ['exportQueueURL'])
failure_queue = sqs.Queue(os.environ['failureQueueURL'])

rdssecret = os.environ['rdsSecretName']
session = boto3.Session()
client = session.client('secretsmanager')
response = client.get_secret_value(SecretId=rdssecret)
decoded = json.loads(response['SecretString'])
rdshost = decoded['rdshost']
username = decoded['username']
password = decoded['password']
dbname = decoded['dbname']
conn = pymysql.connect(host=rdshost, user=username, passwd=password, db=dbname, connect_timeout=5,
                       cursorclass=pymysql.cursors.SSDictCursor)

debug = os.environ['debugEnabled'] == 'Yes'

s3_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
s3_client = boto3.client('s3', config=s3_config)
bua_bucket_name = os.environ['buaBucketName']

handler = BUASiteExportHandler(
    s3_client=s3_client, bua_bucket_name=bua_bucket_name,
    sqs_client=sqs_client, ddb_bua_table=ddb_bua_table,
    export_queue=export_queue, failure_queue=failure_queue,
    conn=conn,
    debug=debug
)


def lambda_handler(event, context):
    try:
        handler.handle_request(event)
    except Exception as ex:
        traceback.print_exception(ex)
        try:
            handler.reconnect(
                pymysql.connect(host=rdshost, user=username, passwd=password, db=dbname, connect_timeout=5,
                                cursorclass=pymysql.cursors.DictCursor))
        except Exception as ex2:
            print('Failed to reconnect to the database after a failure')
            traceback.print_exception(ex2)
        raise RuntimeError(f'Failed to handle request: {event}')
