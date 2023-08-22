import base64
from hashlib import md5

import yaml

from bua.pipeline.facade.sqs import SQS


class BUANextHandler:

    def __init__(self, config, sqs, ddb, s3):
        self.config = config
        self.sqs = SQS(sqs, ddb)
        self.s3 = s3

    def handle_request(self, event):
        if 'Records' in event:
            for record in list(event['Records']):
                print(record)
                if record['eventSource'] == 'aws:s3':
                    text = self._fetch_s3_object(record)
                    event = yaml.load(text, Loader=yaml.Loader)
                    self.handle_request(event)
                if record['eventSource'] == 'aws:sqs':
                    if self.sqs.deduplicate_request(record):
                        event = yaml.load(record['body'], Loader=yaml.Loader)
                        if 'Records' in event:
                            self.handle_request(event)
                        else:
                            self._schedule_event(event)
        else:
            self._schedule_event(event)

    def _fetch_s3_object(self, record):
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        version = record['s3']['object']['versionId']
        print('Fetch S3 object', bucket, key, version)
        response = self.s3.get_object(Bucket=bucket, Key=key, VersionId=version)
        text: str = response['Body'].read().decode('utf-8')
        return text

    def _schedule_event(self, event):
        schedule_name = event['name'].lower().replace(' ', '_')
        next_step = event['this']
        speed = event['steps'][next_step].get('speed', 'fast').lower()
        key = f'schedule/{speed}/{schedule_name}.yml'
        body = yaml.dump(event).encode('utf-8')
        md5sum = base64.b64encode(md5(body).digest()).decode('utf-8')
        print('Schedule event', key)
        self.s3.put_object(
            Bucket=self.config['bucket_name'],
            Key=key,
            ContentMD5=md5sum,
            ContentType='text/plain',
            Body=body,
            ContentLength=len(body)
        )
