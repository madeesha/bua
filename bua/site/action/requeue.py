class SiteRequeue:
    def __init__(self, sqs_client):
        self.sqs_client = sqs_client

    def initiate_requeue(self, source_queue_name, target_queue_name):
        response = self.sqs_client.receive_message(
            QueueUrl=source_queue_name,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )
        while len(response['Messages']) > 0:
            handles = [
                message['ReceiptHandle']
                for message in response['Messages']
            ]
            entries = [
                {
                    'Id': str(index),
                    'MessageBody': message['Body']
                }
                for index, message in enumerate(response['Messages'])
            ]
            response = self.sqs_client.send_message_batch(QueueUrl=target_queue_name, Entries=entries)
            if 'Successful' in response:
                ids = {int(entry['Id']) for entry in response['Successful']}
                entries = [
                    {
                        'Id': str(_id),
                        'ReceiptHandle': handles[_id]
                    }
                    for _id in ids
                ]
                print(f'Requeued {len(ids)} messages from {source_queue_name} to {target_queue_name}')
                self.sqs_client.delete_message_batch(QueueUrl=source_queue_name, Entries=entries)
                response = self.sqs_client.receive_message(
                    QueueUrl=source_queue_name,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=10
                )
