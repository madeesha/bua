from datetime import datetime, timedelta
import time
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


class SQS:
    def __init__(self, sqs_client, ddb_table):
        self.sqs = sqs_client
        self.ddb = ddb_table

    def deduplicate_request(self, record):
        message_id = record['messageId']
        source_arn = record['eventSourceARN']
        when_expires = datetime.today() + timedelta(minutes=15)
        ttl = int(time.mktime(when_expires.timetuple()))
        try:
            self.ddb.put_item(Item={
                'PK': f'X:SQS:{source_arn}',
                'SK': f'{message_id}',
                'TTL': ttl
            }, ConditionExpression=Attr('SK').not_exists())
            return True
        except ClientError as e:
            if e.operation_name == 'PutItem' and e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return False
            raise

    def send_message(self, queue_url, body, delay=0):
        return self.sqs.send_message(QueueUrl=queue_url, MessageBody=body, DelaySeconds=delay)

    def describe_queues(self, queue_name_prefix):
        queues = dict()
        result = self.sqs.list_queues(QueueNamePrefix=queue_name_prefix, MaxResults=1000)
        if 'QueueUrls' in result:
            queues.update({url: {} for url in result['QueueUrls']})
        while 'NextToken' in result and result['NextToken'] is not None:
            result = self.sqs.list_queues(
                QueueNamePrefix=queue_name_prefix, MaxResults=1000, NextToken=result['NextToken']
            )
            if 'QueueUrls' in result:
                queues.update({url: dict() for url in result['QueueUrls']})
        for queue_url, attributes in queues.items():
            attribute_names = [
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
            response = self.sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=attribute_names)
            total = 0
            if 'Attributes' in response:
                for name in attribute_names:
                    attributes[name] = int(response['Attributes'].get(name, 0))
                    total += attributes[name]
            attributes['Total'] = total
        return queues
