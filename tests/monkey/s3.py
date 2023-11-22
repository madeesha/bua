from typing import Dict


class MyS3Bucket:

    def __init__(self):
        self.objects: Dict[str, bytes] = {}

    def put_object(self, key: str, body: bytes):
        self.objects[key] = body


class MyS3:

    def __init__(self):
        self.buckets: Dict[str, MyS3Bucket] = {}

    def get_bucket(self, bucket_name: str) -> MyS3Bucket:
        bucket = self.buckets.get(bucket_name)
        if bucket is None:
            bucket = MyS3Bucket()
            self.buckets[bucket_name] = bucket
        return bucket

    def patch(self):
        self.buckets.clear()


class MonkeyPatchS3Client:

    def __init__(self, mys3: MyS3):
        self.mys3 = mys3

    def put_object(self, **kwargs):
        bucket_name = kwargs['Bucket']
        bucket_key = kwargs['Key']
        body = kwargs['Body']
        bucket = self.mys3.get_bucket(bucket_name)
        bucket.put_object(bucket_key, body)
        return {}

    def upload_fileobj(self, **kwargs):
        file_object = kwargs['Fileobj']
        bucket_name = kwargs['Bucket']
        bucket_key = kwargs['Key']
        body = file_object.read()
        bucket = self.mys3.get_bucket(bucket_name)
        bucket.put_object(bucket_key, body)
        return {}

    def patch(self):
        pass
