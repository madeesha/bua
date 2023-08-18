class SessionEventsStub:
    def emit_until_response(self, *args, signing_name=None, region_name=None, signature_version=None, context=None):
        return None, None

    def emit(self, *args, **kwargs):
        pass


class SessionCredentialsStub:

    def __init__(self):
        self.access_key = '123'
        self.token = '123'
        self.secret_key = '123'

    def get_frozen_credentials(self, *args, **kwargs):
        return self


class SessionStub:
    def __init__(self, clients):
        self.region_name = 'ap-southeast-2'
        self._clients = clients
        self.events = SessionEventsStub()

    def client(self, name, region_name=None):
        return self._clients[name]

    def get_credentials(self):
        return SessionCredentialsStub()
