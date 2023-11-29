from argparse import ArgumentParser

import boto3

parser = ArgumentParser(description='Redrive SQS queue')
parser.add_argument('-p', '--profile', required=True)
parser.add_argument('-s', '--source-queue-url', required=True)
parser.add_argument('-t', '--target-queue-url', required=True)
args = parser.parse_args()

total_received = 0
total_sent = 0
total_deleted = 0

session = boto3.session.Session(region_name='ap-southeast-2', profile_name=args.profile)
sqs_client = session.client('sqs')
receive_response = sqs_client.receive_message(QueueUrl=args.source_queue_url, MaxNumberOfMessages=10)
while 'Messages' in receive_response:
    messages = receive_response['Messages']
    print(f'Received {len(messages)} messages')
    if len(messages) < 1:
        break
    total_received += len(messages)
    entries = [
        {
            'Id': str(ident),
            'MessageBody': message['Body']
        }
        for ident, message in enumerate(messages)
    ]
    send_response = sqs_client.send_message_batch(QueueUrl=args.target_queue_url, Entries=entries)
    successful = send_response.get('Successful', [])
    failed = send_response.get('Failed', [])
    print(f'Sent {len(successful)} messages, {len(failed)} failed to send')
    total_sent += len(successful)
    idents = [int(success['Id']) for success in successful]
    entries = [
        {
            'Id': str(ident),
            'ReceiptHandle': message['ReceiptHandle']
        }
        for ident, message in enumerate(messages)
        if ident in idents
    ]
    if len(entries) > 0:
        delete_response = sqs_client.delete_message_batch(QueueUrl=args.source_queue_url, Entries=entries)
        successful = delete_response.get('Successful', [])
        failed = delete_response.get('Failed', [])
        print(f'Deleted {len(successful)} messages, {len(failed)} failed to send')
        total_deleted += len(successful)
    receive_response = sqs_client.receive_message(QueueUrl=args.source_queue_url, MaxNumberOfMessages=10)
print(f'{total_received} received , {total_sent} sent , {total_deleted} deleted')
