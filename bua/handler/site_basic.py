import os
from bua.site.handler.basic import BUASiteBasicHandler
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
basic_queue = sqs.Queue(os.environ['basicQueueURL'])
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
ctl_conn = pymysql.connect(host=rdshost, user=username, passwd=password, db=dbname, connect_timeout=5,
                           cursorclass=pymysql.cursors.SSDictCursor)

debug = os.environ['debugEnabled'] == 'Yes'

s3_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
s3_client = boto3.client('s3', config=s3_config)
meterdata_bucket_name = os.environ['meterdataBucketName']
max_receive_count = int(os.environ['maxReceiveCount'])

handler = BUASiteBasicHandler(
    s3_client=s3_client, meterdata_bucket_name=meterdata_bucket_name,
    sqs_client=sqs_client, ddb_meterdata_table=ddb_meterdata_table, ddb_bua_table=ddb_bua_table,
    basic_queue=basic_queue, failure_queue=failure_queue,
    conn=conn, ctl_conn=ctl_conn,
    debug=debug, max_receive_count=max_receive_count
)


def lambda_handler(event, context):
    try:
        handler.handle_request(event)
    except Exception as ex:
        traceback.print_exception(ex)
        try:
            handler.reconnect(
                pymysql.connect(host=rdshost, user=username, passwd=password, db=dbname, connect_timeout=5,
                                cursorclass=pymysql.cursors.DictCursor),
                pymysql.connect(host=rdshost, user=username, passwd=password, db=dbname, connect_timeout=5,
                                cursorclass=pymysql.cursors.DictCursor)
            )
        except Exception as ex2:
            handler.log('Failed to reconnect to the database after a failure')
            traceback.print_exception(ex2)
        raise RuntimeError(f'Failed to handle request: {event}')
