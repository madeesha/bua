class MonkeyPatchSession:
    def __init__(self, clients, resources):
        self._clients = clients
        self._resources = resources

    def client(self, name, *args, **kwargs):
        return self._clients[name]

    def resource(self, name, *args, **kwargs):
        return self._resources[name]

    @property
    def region_name(self):
        return ''

    def patch(self):
        pass
