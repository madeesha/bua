class S3ClientStub:

    def put_object(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Bucket', 'Key', 'ContentMD5', 'ContentType', 'ContentLength', 'Body'}
        return {}

    def get_object(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Bucket', 'Key'}
        return {}
