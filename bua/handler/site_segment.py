import os
from bua.site.handler.segment import BUASiteSegmentHandler
import boto3
import botocore.config
import json
import pymysql
import pymysql.cursors
import traceback
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

ddb_config = botocore.config.Config(max_pool_connections=10, connect_timeout=10, read_timeout=30)
ddb = boto3.resource('dynamodb', config=ddb_config)
table = ddb.Table(os.environ['tableName'])

sqs_config = botocore.config.Config(max_pool_connections=10, connect_timeout=10, read_timeout=30)
sqs = boto3.resource('sqs', config=sqs_config, endpoint_url='https://sqs.ap-southeast-2.amazonaws.com')
segment_queue = sqs.Queue(os.environ['segmentQueueURL'])
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
                       cursorclass=pymysql.cursors.DictCursor)

db_url = URL.create(drivername='mysql+pymysql', host=rdshost, username=username, password=password, port=3306,
                    database=dbname)
db_engine = create_engine(db_url)

debug = os.environ['debugEnabled'] == 'Yes'

s3_config = botocore.config.Config(connect_timeout=10, read_timeout=30)
s3_client = boto3.client('s3', config=s3_config)
meterdata_bucket_name = os.environ['meterdataBucketName']
bua_bucket_name = os.environ['buaBucketName']

handler = BUASiteSegmentHandler(
    s3_client=s3_client, meterdata_bucket_name=meterdata_bucket_name, bua_bucket_name=bua_bucket_name,
    table=table,
    segment_queue=segment_queue, failure_queue=failure_queue,
    conn=conn, db_engine=db_engine,
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
