import os
import boto3

topic_arn = os.getenv('topic_arn')
message = os.getenv('message')

client = boto3.client('sns', region_name='ap-southeast-2')

client.publish(TopicArn=topic_arn, Message=message)
