class MonkeyPatchS3Client:

    def put_object(self, *args, **kwargs):
        return {}

    def patch(self):
        pass
