class SSMClientStub:

    def get_parameters(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Names', 'WithDecryption'}
        return {
            'Parameters': [],
            'InvalidParameters': []
        }
