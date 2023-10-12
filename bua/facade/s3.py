from typing import List


class S3:

    copy_optionals = {
        'sse': 'ServerSideEncryption',
        'kms_key_id': 'SSEKMSKeyId',
        'expected_bucket_owner': 'ExpectedBucketOwner',
        'expected_source_bucket_owner': 'ExpectedSourceBucketOwner',
    }

    def __init__(self, *, s3_client):
        self.s3_client = s3_client

    def list_objects(self, *, bucket_name: str, bucket_prefix: str) -> List[str]:
        """Get a list of object keys from the S3 bucket"""
        objects = []
        response = self.s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=bucket_prefix
        )
        self._append_object_keys(objects, response)
        while 'NextContinuationToken' in response and 'IsTruncated' in response and response['IsTruncated'] is True:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=bucket_prefix,
                ContinuationToken=response['NextContinuationToken']
            )
            self._append_object_keys(objects, response)
        return objects

    def copy_object(self, *, source_bucket: str, source_key: str, target_bucket: str, target_key: str, **kwargs):
        copy_args = {
            'Bucket': target_bucket,
            'Key': target_key,
            'CopySource': {
                'Bucket': source_bucket,
                'Key': source_key
            }
        }
        for key, value in self.copy_optionals.items():
            if key in kwargs:
                copy_args[value] = kwargs[key]
        self.s3_client.copy_object(**copy_args)

    @staticmethod
    def _append_object_keys(objects, response):
        if 'Contents' in response:
            objects.extend([
                content['Key']
                for content in response['Contents']
                if 'Key' in content and 'StorageClass' in content and content['StorageClass'] == 'STANDARD'
            ])

    def upload_fileobj(self, *, fp, bucket_name: str, key: str):
        self.s3_client.upload_fileobj(Fileobj=fp, Bucket=bucket_name, Key=key)

    def delete_object(self, *, bucket_name: str, bucket_key: str):
        self.s3_client.delete_object(Bucket=bucket_name, Key=bucket_key)
