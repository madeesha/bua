import os
from bua.site.handler.initiate import BUASiteInitiateHandler
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

s3_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
s3_client = boto3.client('s3', config=s3_config)

sqs_config = botocore.config.Config(max_pool_connections=10, connect_timeout=10, read_timeout=30)
sqs = boto3.resource('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
sqs_client = boto3.client('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
data_queue = sqs.Queue(os.environ['dataQueueURL'])
segment_queue = sqs.Queue(os.environ['segmentQueueURL'])
export_queue = sqs.Queue(os.environ['exportQueueURL'])
failure_queue = sqs.Queue(os.environ['failureQueueURL'])
basic_queue = sqs.Queue(os.environ['basicQueueURL'])
mscalar_queue = sqs.Queue(os.environ['mscalarQueueURL'])
prepare_queue = sqs.Queue(os.environ['prepareQueueURL'])
nem12_queue = sqs.Queue(os.environ['nem12QueueURL'])

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
util_batch_size = int(os.environ['utilityBatchSize'])
jur_batch_size = int(os.environ['jurisdictionBatchSize'])
tni_batch_size = int(os.environ['tniBatchSize'])

max_receive_count = int(os.environ['maxReceiveCount'])

handler = BUASiteInitiateHandler(
    sqs_client=sqs_client,
    s3_client=s3_client,
    ddb_meterdata_table=ddb_meterdata_table, ddb_bua_table=ddb_bua_table,
    data_queue=data_queue, segment_queue=segment_queue, export_queue=export_queue, failure_queue=failure_queue,
    basic_queue=basic_queue, mscalar_queue=mscalar_queue, prepare_queue=prepare_queue, nem12_queue=nem12_queue,
    conn=conn, debug=debug,
    util_batch_size=util_batch_size, jur_batch_size=jur_batch_size, tni_batch_size=tni_batch_size,
    max_receive_count=max_receive_count
)


def lambda_handler(event, context):
    try:
        handler.handle_request(event)
    except Exception as ex:
        traceback.print_exception(ex)
        try:
            handler.reconnect(
                pymysql.connect(host=rdshost, user=username, passwd=password, db=dbname, connect_timeout=5,
                                cursorclass=pymysql.cursors.SSDictCursor))
        except Exception as ex2:
            handler.log('Failed to reconnect to the database after a failure')
            traceback.print_exception(ex2)
        raise RuntimeError(f'Failed to handle request: {event}')
