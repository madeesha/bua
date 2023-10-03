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
ddb_table = ddb.Table(os.environ['tableName'])

sqs_config = botocore.config.Config(max_pool_connections=10, connect_timeout=10, read_timeout=30)
sqs = boto3.resource('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
sqs_client = boto3.client('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
data_queue = sqs.Queue(os.environ['dataQueueURL'])
segment_queue = sqs.Queue(os.environ['segmentQueueURL'])

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

handler = BUASiteInitiateHandler(
    sqs_client=sqs_client, ddb_table=ddb_table,
    data_queue=data_queue, segment_queue=segment_queue,
    conn=conn, debug=debug,
    util_batch_size=util_batch_size, jur_batch_size=jur_batch_size, tni_batch_size=tni_batch_size
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
            print('Failed to reconnect to the database after a failure')
            traceback.print_exception(ex2)
        raise RuntimeError(f'Failed to handle request: {event}')
