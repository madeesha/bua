class CFClientStub:

    def __init__(self, describe_stack_responses):
        self.describe_stack_responses = describe_stack_responses

    def describe_stacks(self, **kwargs):
        for key in kwargs.keys():
            assert key in {'StackName'}
        return self.describe_stack_responses[kwargs['StackName']]

    def delete_stack(self, **kwargs):
        for key in kwargs.keys():
            assert key in {'StackName'}
