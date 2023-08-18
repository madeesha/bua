class DDBTableStub:

    def put_item(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Item', 'ConditionExpression'}
        return {}

    def get_item(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'Key', 'ConsistentRead'}
        return {}
