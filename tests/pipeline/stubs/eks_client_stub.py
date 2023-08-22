class EKSClientStub:
    def describe_cluster(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'name'}
        return {
            'cluster': {
                'arn': '123',
                'certificateAuthority': {
                    'data': '123'
                },
                'endpoint': '123'
            }
        }
