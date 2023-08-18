class CFClientStub:
    def describe_stacks(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'StackName'}
        return {
            'Stacks': [
                {

                }
            ]
        }
