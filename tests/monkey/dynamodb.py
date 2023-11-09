class MonkeyPatchDynamoDBResource:
    def __init__(self):
        self._table = MonkeyPatchTable()

    def Table(self, *args, **kwargs):
        return self._table

    def patch(self):
        self._table.patch()


class MonkeyPatchTable:

    def get_item(self, *args, **kwargs):
        return {}

    def put_item(self, *args, **kwargs):
        return {}

    def delete_item(self, *args, **kwargs):
        return {}

    def patch(self):
        pass
