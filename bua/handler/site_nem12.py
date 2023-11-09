import os

from bua.facade.connection import DBProxy
from bua.site.handler.nem12 import BUASiteNEM12Handler
import boto3
import botocore.config
import json
import pymysql
import pymysql.cursors
import traceback

ddb_config = botocore.config.Config(max_pool_connections=10, connect_timeout=10, read_timeout=30)
ddb = boto3.resource('dynamodb', config=ddb_config)
ddb_meterdata_table = ddb.Table(os.environ['meterdataTableName'])
ddb_bua_table = ddb.Table(os.environ['buaTableName'])

sqs_config = botocore.config.Config(max_pool_connections=10, connect_timeout=10, read_timeout=30)
sqs = boto3.resource('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
sqs_client = boto3.client('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
nem12_queue = sqs.Queue(os.environ['nem12QueueURL'])
failure_queue = sqs.Queue(os.environ['failureQueueURL'])

rdssecret = os.environ['rdsSecretName']
session = boto3.Session()
client = session.client('secretsmanager')
response = client.get_secret_value(SecretId=rdssecret)
decoded = json.loads(response['SecretString'])
username = decoded['username']
password = decoded['password']

conn = DBProxy(mysql=pymysql, username=username, password=password, lock_wait_timeout=900)
ctl_conn = DBProxy(mysql=pymysql, username=username, password=password, lock_wait_timeout=900)

debug = os.environ['debugEnabled'] == 'Yes'

s3_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
s3_client = boto3.client('s3', config=s3_config)
meterdata_bucket_name = os.environ['meterdataBucketName']

max_receive_count = int(os.environ['maxReceiveCount'])

handler = BUASiteNEM12Handler(
    s3_client=s3_client, meterdata_bucket_name=meterdata_bucket_name,
    sqs_client=sqs_client, ddb_meterdata_table=ddb_meterdata_table, ddb_bua_table=ddb_bua_table,
    nem12_queue=nem12_queue, failure_queue=failure_queue,
    conn=conn, ctl_conn=ctl_conn,
    debug=debug,
    max_receive_count=max_receive_count
)


def lambda_handler(event, context):
    handler.handle_request(event)
