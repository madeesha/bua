import json
from datetime import datetime, timedelta
import time
from typing import List, Dict

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

    def empty_queue(self, queue_url):
        self.sqs.purge_queue(QueueUrl=queue_url)


class Queue:
    def __init__(self, queue, debug, log):
        self.queue = queue
        self.debug = debug
        self._log = log

    def send_failure_event(self, event: Dict, cause: str):
        body = {
            'event': event,
            'cause': cause
        }
        response = self.queue.send_message(MessageBody=json.dumps(body))
        message_id = response['MessageId']
        self._log(f'Sent failure message {message_id} because [{cause}]')

    def send_if_needed(self, bodies: list, force=False, batch_size=10):
        """Send SQS message batches if needed"""
        if len(bodies) >= (batch_size*10) or (len(bodies) > 0 and force):
            batches = [
                {'entries': [bodies[n] for n in range(index, min(index+batch_size, len(bodies)))]}
                for index in range(0, len(bodies), batch_size)
            ]
            for index in range(0, len(batches), 10):
                self.send_request(batches[index:index+10])
            bodies.clear()

    def send_request(self, bodies: List):
        """Send an SQS message batch"""
        entries = [{'Id': str(index), 'MessageBody': json.dumps(body)} for index, body in enumerate(bodies)]
        response = self.queue.send_messages(Entries=entries)
        if self.debug:
            if 'Successful' in response:
                self._log(f'Sent {len(response["Successful"])} messages')
        while 'Failed' in response and len(response['Failed']) > 0:
            for failure in response['Failed']:
                self._log(f'Failed {failure["Id"]} : '
                      f'Sender fault {failure["SenderFault"]} : {failure["Code"]} : {failure["Message"]}')
            failures = {entry['Id'] for entry in response['Failed']}
            entries = [entry for entry in entries if entry['Id'] in failures]
            response = self.queue.send_messages(Entries=entries)
            if self.debug:
                if 'Successful' in response:
                    self._log(f'Sent {len(response["Successful"])} messages')
